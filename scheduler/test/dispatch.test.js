// Tests for the scheduler Worker. No network: fetch is replaced.
//
// Run from the repository root:  node --test scheduler/test/

import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

import worker, { again, dispatch, expiry, held, runId } from '../src/index.js';

const ENV = {
	GITHUB_TOKEN: 'not-a-real-token',
	GITHUB_REPOSITORY: 'joaopasbento/eupowerdata-feed',
	WORKFLOW_FILE: 'fetch.yml',
	GIT_REF: 'main',
	CHECK_AFTER_SECONDS: '0',          // the tests do not wait the two minutes
};
const realFetch = globalThis.fetch;
const noWait = async () => {};

/** Replace fetch with one that gives these answers in turn, and records the calls. */
function github(...answers) {
	const calls = [];
	globalThis.fetch = async (url, options) => {
		calls.push({ url, options });
		const answer = answers[Math.min(calls.length, answers.length) - 1];
		if (answer instanceof Error) {
			throw answer;
		}
		return new Response(answer.body ?? null, { status: answer.status, headers: answer.headers });
	};
	return calls;
}

afterEach(() => {
	globalThis.fetch = realFetch;
});

test('asks GitHub to run fetch.yml on main', async () => {
	const calls = github({ status: 200, body: '{"workflow_run_id":1,"html_url":"https://github.com/x/y/actions/runs/1"}' });
	const answer = await dispatch(ENV, noWait);

	assert.equal(answer.status, 200);
	assert.equal(calls.length, 1);
	assert.equal(calls[0].url,
		'https://api.github.com/repos/joaopasbento/eupowerdata-feed/actions/workflows/fetch.yml/dispatches');
	assert.equal(calls[0].options.method, 'POST');
	assert.deepEqual(JSON.parse(calls[0].options.body), { ref: 'main' });
	const headers = calls[0].options.headers;
	assert.equal(headers.Authorization, 'Bearer not-a-real-token');
	assert.equal(headers.Accept, 'application/vnd.github+json');
	assert.equal(headers['X-GitHub-Api-Version'], '2026-03-10');
	assert.match(headers['User-Agent'], /^EUPowerData-Feed-Scheduler\/1\.0 \(\+https:\/\/eupowerdata\.com\)$/);
});

test('an empty answer (HTTP 204) is a run started too', async () => {
	github({ status: 204 });
	assert.equal((await dispatch(ENV, noWait)).status, 204);
});

test('an expired or revoked token fails at once, and says so', async () => {
	const calls = github({ status: 401, body: '{"message":"Bad credentials"}' });
	await assert.rejects(dispatch(ENV, noWait), (error) => {
		assert.match(error.message, /HTTP 401/);
		assert.match(error.message, /has expired/);
		assert.doesNotMatch(error.message, /not-a-real-token/);
		return true;
	});
	assert.equal(calls.length, 1);          // no retry: the answer would be the same
});

test('a token without the permission fails at once, and says which it needs', async () => {
	for (const status of [403, 404]) {
		const calls = github({ status, body: '{"message":"Resource not accessible by personal access token"}' });
		await assert.rejects(dispatch(ENV, noWait), /Actions: Read and write/);
		assert.equal(calls.length, 1);
	}
});

test('a GitHub server error is retried once', async () => {
	const calls = github({ status: 502, body: 'Bad gateway' }, { status: 200, body: '{}' });
	assert.equal((await dispatch(ENV, noWait)).status, 200);
	assert.equal(calls.length, 2);
});

test('a network error is retried once, then fails', async () => {
	const calls = github(new Error('connection reset'));
	await assert.rejects(dispatch(ENV, noWait), /could not reach GitHub \(connection reset\)/);
	assert.equal(calls.length, 2);
});

test('without the secret nothing is sent', async () => {
	const calls = github({ status: 200 });
	await assert.rejects(dispatch({ ...ENV, GITHUB_TOKEN: undefined }, noWait), /GITHUB_TOKEN is not set/);
	assert.equal(calls.length, 0);
});

test('the Cron Trigger starts a run, and fails loudly when it cannot', async () => {
	const controller = { scheduledTime: Date.UTC(2026, 10, 16, 9, 7), cron: '7,22,37,52 * * * *' };
	const lines = [];
	const { log, error } = console;
	console.log = (line) => lines.push(line);
	console.error = (line) => lines.push(line);
	try {
		github({ status: 200, body: '{"html_url":"https://github.com/x/y/actions/runs/1"}' });
		await worker.scheduled(controller, ENV, {});
		assert.match(lines[0], /^Started fetch\.yml on main of joaopasbento\/eupowerdata-feed \(due 2026-11-16T09:07:00\.000Z, HTTP 200\) https:/);
		assert.match(lines[0], /; the token has no expiration date$/);

		github({ status: 401, body: '{"message":"Bad credentials"}' });
		await assert.rejects(worker.scheduled(controller, ENV, {}), /Workflow not started/);
		assert.match(lines.at(-1), /HTTP 401/);
		assert.ok(lines.every((line) => !line.includes('not-a-real-token')));
	} finally {
		console.log = log;
		console.error = error;
	}
});

test('the log says when the token expires, and warns in the last two weeks', async () => {
	const now = Date.UTC(2026, 10, 16, 11, 0);
	assert.deepEqual(expiry('2027-11-15 10:59:13 UTC', now),
		{ text: 'the token expires 2027-11-15 10:59:13 UTC, in 363 days', soon: false });
	assert.deepEqual(expiry('2026-11-25 10:59:13 UTC', now),
		{ text: 'the token expires 2026-11-25 10:59:13 UTC, in 8 days', soon: true });
	assert.deepEqual(expiry('2026-11-25 10:59:13 +0000', now).soon, true);
	assert.deepEqual(expiry(null, now), { text: 'the token has no expiration date', soon: false });
	assert.deepEqual(expiry('some other format', now), { text: 'the token expires some other format', soon: false });

	const lines = [];
	const { log, warn } = console;
	console.log = (line) => lines.push(line);
	console.warn = (line) => lines.push(`WARN ${line}`);
	try {
		const soon = new Date(Date.now() + 5 * 86400000).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
		github({ status: 200, body: '{}', headers: { 'github-authentication-token-expiration': soon } });
		await worker.scheduled({ scheduledTime: Date.now() }, ENV, {});
		assert.match(lines[0], /; the token expires .* UTC, in 4 days$/);
		assert.match(lines[1], /^WARN Renew the GitHub token/);
	} finally {
		console.log = log;
		console.warn = warn;
	}
});

// --- Asking again when GitHub holds a run ---

const REPO = 'https://api.github.com/repos/joaopasbento/eupowerdata-feed';
const RUNS = 'https://github.com/joaopasbento/eupowerdata-feed/actions/runs';
const STARTED = { status: 200, body: `{"workflow_run_id":101,"html_url":"${RUNS}/101"}` };
const SECOND = { status: 200, body: `{"workflow_run_id":102,"html_url":"${RUNS}/102"}` };
const run = (status) => ({ status: 200, body: JSON.stringify({ id: 101, status }) });
const going = (count) => ({ status: 200, body: JSON.stringify({ total_count: count, workflow_runs: [] }) });
const LAP = { scheduledTime: Date.UTC(2026, 10, 17, 9, 37, 52), cron: '7,22,37,52 * * * *' };

/** One turn of the schedule with GitHub giving these answers in turn: {calls, lines}. */
async function turn(...answers) {
	const calls = github(...answers);
	const lines = [];
	const original = { log: console.log, warn: console.warn, error: console.error };
	console.log = (line) => lines.push(`LOG ${line}`);
	console.warn = (line) => lines.push(`WARN ${line}`);
	console.error = (line) => lines.push(`ERROR ${line}`);
	try {
		await worker.scheduled(LAP, ENV, {});
	} finally {
		Object.assign(console, original);
	}
	assert.ok(lines.every((line) => !line.includes('not-a-real-token')));
	return { calls, lines, posts: calls.filter((call) => call.options.method === 'POST').length };
}

test('the run starts in time: it is looked at once and not asked for again', async () => {
	for (const status of ['in_progress', 'completed']) {
		const { calls, lines, posts } = await turn(STARTED, run(status));
		assert.equal(posts, 1);
		assert.equal(calls.length, 2);
		assert.equal(calls[1].url, `${REPO}/actions/runs/101`);
		assert.equal(calls[1].options.method, undefined);                    // a read
		assert.equal(calls[1].options.headers.Authorization, 'Bearer not-a-real-token');
		assert.match(lines[1], new RegExp(`^LOG Run 101 \\(due 2026-11-17T09:37:52\\.000Z\\) two minutes on: it has started \\(${status}\\) - not asking again$`));
	}
});

test('the run is held with nothing in its way: it is asked for again, once', async () => {
	const { calls, lines, posts } = await turn(STARTED, run('queued'), going(0), SECOND, run('queued'), going(0), SECOND);
	assert.equal(posts, 2);                                                  // the first request and one more
	assert.equal(calls.length, 4);                                           // and the second run is not looked at
	assert.equal(calls[2].url, `${REPO}/actions/workflows/fetch.yml/runs?status=in_progress&per_page=1`);
	assert.equal(calls[3].url, `${REPO}/actions/workflows/fetch.yml/dispatches`);
	assert.deepEqual(JSON.parse(calls[3].options.body), { ref: 'main' });
	assert.match(lines[1], new RegExp(`^WARN Run 101 \\(due [^)]+\\) two minutes on: it has not started and no other run is going - asked again: ${RUNS}/102$`));
});

test('the run is waiting its turn behind another: it is not asked for again', async () => {
	// As GitHub shows a run waiting for the concurrency group...
	let done = await turn(STARTED, run('pending'));
	assert.equal(done.posts, 1);
	assert.equal(done.calls.length, 2);
	assert.match(done.lines[1], /waiting for its turn behind another run \(pending\) - not asking again$/);

	// ...and, to be sure, a run not started while another run of the workflow is going.
	done = await turn(STARTED, run('queued'), going(1));
	assert.equal(done.posts, 1);
	assert.equal(done.calls.length, 3);
	assert.match(done.lines[1], /another run is going, so it is waiting for its turn - not asking again$/);
});

test('GitHub cannot be asked about the run: it is not asked for again, and the log says so', async () => {
	for (const failure of [{ status: 500, body: 'Server Error' }, { status: 403, body: '{}' }, new Error('connection reset')]) {
		const { calls, lines, posts } = await turn(STARTED, failure);
		assert.equal(posts, 1);
		assert.equal(calls.length, 2);
		assert.match(lines[1], /^ERROR Could not look at run 101 \(due [^)]+\): .* - not asking again$/);
	}
	// The same when it is the question about other runs that fails.
	const { calls, lines, posts } = await turn(STARTED, run('queued'), { status: 502, body: 'Bad gateway' });
	assert.equal(posts, 1);
	assert.equal(calls.length, 3);
	assert.match(lines[1], /^ERROR Could not look at run 101 .*HTTP 502.* - not asking again$/);
});

test('a state GitHub has not shown before is not taken for a held run', async () => {
	for (const status of ['waiting', 'requested', undefined]) {
		const { posts, lines } = await turn(STARTED, run(status));
		assert.equal(posts, 1);
		assert.match(lines[1], /not one this knows as held - not asking again$/);
	}
});

test('when GitHub does not say which run it created there is nothing to look at', async () => {
	const { calls, lines } = await turn({ status: 204 });
	assert.equal(calls.length, 1);
	assert.match(lines[1], /^WARN GitHub did not say which run it created/);
});

test('when asking again fails, the log says so and the turn still ends well', async () => {
	const { calls, lines, posts } = await turn(STARTED, run('queued'), going(0), { status: 422, body: '{"message":"no"}' });
	assert.equal(posts, 2);
	assert.equal(calls.length, 4);
	assert.match(lines[1], /^ERROR Run 101 \(due [^)]+\) is held and asking again failed: Workflow not started: GitHub answered HTTP 422/);
});

test('it waits two minutes before looking, or what CHECK_AFTER_SECONDS says', async () => {
	const waited = [];
	const wait = async (ms) => waited.push(ms);
	const { log } = console;
	console.log = () => {};
	try {
		for (const setting of [undefined, '', 'soon', '-5', '120', '45']) {
			github(run('in_progress'));
			await again({ ...ENV, CHECK_AFTER_SECONDS: setting }, '101', 'now', wait);
		}
	} finally {
		console.log = log;
	}
	assert.deepEqual(waited, [120000, 120000, 120000, 120000, 120000, 45000]);
});

test('held() tells a held run from the others', async () => {
	github(run('queued'), going(0));
	assert.deepEqual(await held(ENV, '101'), { held: true, why: 'it has not started and no other run is going' });
	github(run('queued'), going(2));
	assert.equal((await held(ENV, '101')).held, false);
	github(run('in_progress'));
	assert.equal((await held(ENV, '101')).held, false);
	github({ status: 404, body: '{}' });
	await assert.rejects(held(ENV, '101'), /HTTP 404 to actions\/runs\/101/);
});

test('the run GitHub created is read from its answer', () => {
	assert.equal(runId(STARTED.body), '101');
	assert.equal(runId(`{"html_url":"${RUNS}/43857237300"}`), '43857237300');
	assert.equal(runId('{"run_url":"https://api.github.com/repos/x/y/actions/runs/7"}'), '7');
	assert.equal(runId(''), null);
	assert.equal(runId('{}'), null);
	assert.equal(runId('not json'), null);
});

test('the Worker has no address: only a scheduled handler', () => {
	assert.deepEqual(Object.keys(worker), ['scheduled']);
});
