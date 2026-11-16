// Tests for the scheduler Worker. No network: fetch is replaced.
//
// Run from the repository root:  node --test scheduler/test/

import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

import worker, { dispatch } from '../src/index.js';

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
		return new Response(answer.body ?? null, { status: answer.status });
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

		github({ status: 401, body: '{"message":"Bad credentials"}' });
		await assert.rejects(worker.scheduled(controller, ENV, {}), /Workflow not started/);
		assert.match(lines[1], /HTTP 401/);
		assert.ok(lines.every((line) => !line.includes('not-a-real-token')));
	} finally {
		console.log = log;
		console.error = error;
	}
});

test('the Worker has no address: only a scheduled handler', () => {
	assert.deepEqual(Object.keys(worker), ['scheduled']);
});
