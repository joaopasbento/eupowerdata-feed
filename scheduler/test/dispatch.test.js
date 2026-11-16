// Tests for the scheduler Worker. No network: fetch is replaced.
//
// Run from the repository root:  node --test scheduler/test/

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { afterEach, test } from 'node:test';

import worker, { dispatch, expiry, variant } from '../src/index.js';

const ENV = {
	GITHUB_TOKEN: 'not-a-real-token',
	GITHUB_REPOSITORY: 'joaopasbento/eupowerdata-feed',
	WORKFLOW_FILE: 'fetch.yml',
	GIT_REF: 'main',
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
		assert.match(lines[1], /HTTP 401/);
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

// --- The experiment on what makes GitHub hold some runs ---

const lap = (hour, minute) => Date.UTC(2026, 10, 16, hour, minute, 52, 515);

test('every hour has the four combinations once', () => {
	for (let hour = 0; hour < 24; hour++) {
		const labels = [7, 22, 37, 52].map((minute) => variant(lap(hour, minute)).label).sort();
		assert.deepEqual(labels, ['api-new alone', 'api-new reserve', 'api-old alone', 'api-old reserve'], `hour ${hour}`);
	}
});

test('over four hours each combination falls on each of the four minutes', () => {
	const minutes = {};
	for (let hour = 12; hour < 16; hour++) {
		for (const minute of [7, 22, 37, 52]) {
			(minutes[variant(lap(hour, minute)).label] ??= []).push(minute);
		}
	}
	for (const [label, seen] of Object.entries(minutes)) {
		assert.deepEqual(seen.sort((a, b) => a - b), [7, 22, 37, 52], label);
	}
});

test('"reserve" is exactly when the cron lines of fetch.yml have a run due', () => {
	const workflow = readFileSync(new URL('../../.github/workflows/fetch.yml', import.meta.url), 'utf8');
	const crons = [...workflow.matchAll(/^\s*- cron: '([^']+)'/gm)].map((found) => found[1]);
	assert.deepEqual(crons, ['7,37 */2 * * *', '22,52 1-23/2 * * *']);
	const due = (hour, minute) => crons.some((cron) => {
		const [minutes, hours] = cron.split(' ');
		const odd = hours.startsWith('1-');                        // "*/2" is the even hours, "1-23/2" the odd
		return minutes.split(',').includes(String(minute)) && (hour % 2 === 1) === odd;
	});
	for (let hour = 0; hour < 24; hour++) {
		for (const minute of [7, 22, 37, 52]) {
			assert.equal(variant(lap(hour, minute)).label.endsWith('reserve'), due(hour, minute), `${hour}:${minute}`);
		}
	}
});

test('the variant goes in the request: the API version in the header, the label as an input', async () => {
	const env = { ...ENV, EXPERIMENT: 'on' };
	const lines = [];
	const { log } = console;
	console.log = (line) => lines.push(line);
	try {
		// 12:37 UTC: the old API version, with a scheduled run due the same minute.
		let calls = github({ status: 204 });
		await worker.scheduled({ scheduledTime: lap(12, 37) }, env, {});
		assert.equal(calls[0].options.headers['X-GitHub-Api-Version'], '2022-11-28');
		assert.deepEqual(JSON.parse(calls[0].options.body), { ref: 'main', inputs: { variant: 'api-old reserve' } });
		assert.match(lines[0], /^Started fetch\.yml on main of joaopasbento\/eupowerdata-feed \[api-old reserve, API 2022-11-28\] \(due 2026-11-16T12:37:52\.515Z, HTTP 204\)/);

		// 13:07 UTC: the new one, alone.
		calls = github({ status: 200, body: '{}' });
		await worker.scheduled({ scheduledTime: lap(13, 7) }, env, {});
		assert.equal(calls[0].options.headers['X-GitHub-Api-Version'], '2026-03-10');
		assert.deepEqual(JSON.parse(calls[0].options.body), { ref: 'main', inputs: { variant: 'api-new alone' } });
	} finally {
		console.log = log;
	}
});

test('with the experiment off every run is asked for the same way, without inputs', async () => {
	for (const setting of [undefined, 'off', '']) {
		const calls = github({ status: 200, body: '{}' });
		const { log } = console;
		console.log = () => {};
		try {
			await worker.scheduled({ scheduledTime: lap(12, 37) }, { ...ENV, EXPERIMENT: setting }, {});
		} finally {
			console.log = log;
		}
		assert.equal(calls[0].options.headers['X-GitHub-Api-Version'], '2026-03-10');
		assert.deepEqual(JSON.parse(calls[0].options.body), { ref: 'main' });
	}
});

test('the Worker has no address: only a scheduled handler', () => {
	assert.deepEqual(Object.keys(worker), ['scheduled']);
});
