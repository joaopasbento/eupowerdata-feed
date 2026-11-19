/**
 * EU Power Data feed scheduler: a Cloudflare Worker that starts a run of the
 * feed's workflow (.github/workflows/fetch.yml) every 15 minutes.
 *
 * GitHub's own schedule drops most of its runs: between 22:33 UTC on 15 Nov
 * 2026 and 07:48 on the 16th it started one, of the 37 that were due. This
 * Worker is the clock instead. On each Cron Trigger it asks GitHub to run the
 * workflow on the main branch, through the "workflow dispatch" endpoint:
 *
 *   POST /repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches
 *   https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
 *
 * Asking again. GitHub creates the run at once but now and then leaves it for
 * a long time before starting it, with no other run in its way: 14 to 65
 * minutes, in 5 of the 85 runs of 16 and 17 Nov 2026. A request made while a
 * run is held like that starts at once all the same. So, two minutes after
 * asking, the Worker looks at the run and, if it is still where it was, asks
 * once more (see held() and scheduled() below). It does that inside the same
 * invocation, which simply waits the two minutes: nothing to store, no second
 * trigger, and the run to look at is the one GitHub named in its answer. An
 * invocation may wait up to 15 minutes and waiting takes no CPU time; if one
 * is ever cut short, all that is lost is that one second request.
 *
 * It has no address of its own (no workers.dev address, no route). Besides
 * its Cron Trigger, the only way in is a service binding from the site's own
 * Worker (eupowerdata-site), whose status page, behind its PIN, has buttons
 * that ask for a run now: see onRequest() below. A service binding cannot be
 * reached from the internet, so the GitHub token never leaves this Worker and
 * the site needs no key of its own.
 *
 * Nothing secret is in this folder. The GitHub token is a secret of the
 * Worker, set in the Cloudflare dashboard under the name GITHUB_TOKEN; what
 * it must be allowed to do is in the README.
 */

const API = 'https://api.github.com';
const API_VERSION = '2026-03-10';
const USER_AGENT = 'EUPowerData-Feed-Scheduler/1.0 (+https://eupowerdata.com)';
const ATTEMPTS = 2;           // one retry, after a network error or an HTTP 5xx
const RETRY_AFTER_MS = 3000;
const WARN_DAYS = 14;         // how long before the token expires the log starts warning
const CHECK_AFTER_S = 120;    // how long after asking for a run the Worker looks at it (CHECK_AFTER_SECONDS)

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * One request to GitHub. Returns {status, body, expires}; throws on a network
 * error. `expires` is when GitHub says the token stops working (it sends that
 * with every answer to a token that has an expiration date), or null.
 */
async function post(env, workflow = env.WORKFLOW_FILE, inputs = null) {
	const url = `${API}/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${workflow}/dispatches`;
	const response = await fetch(url, {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${env.GITHUB_TOKEN}`,
			Accept: 'application/vnd.github+json',
			'Content-Type': 'application/json',
			'X-GitHub-Api-Version': API_VERSION,
			'User-Agent': USER_AGENT,
		},
		body: JSON.stringify(inputs ? { ref: env.GIT_REF, inputs } : { ref: env.GIT_REF }),
	});
	return {
		status: response.status,
		body: (await response.text()).slice(0, 500),
		expires: response.headers.get('github-authentication-token-expiration'),
	};
}

/** One read from GitHub's API: the parsed answer. Throws when it is not a 200 with JSON. */
async function read(env, path) {
	const response = await fetch(`${API}/repos/${env.GITHUB_REPOSITORY}/${path}`, {
		headers: {
			Authorization: `Bearer ${env.GITHUB_TOKEN}`,
			Accept: 'application/vnd.github+json',
			'X-GitHub-Api-Version': API_VERSION,
			'User-Agent': USER_AGENT,
		},
	});
	if (response.status !== 200) {
		throw new Error(`GitHub answered HTTP ${response.status} to ${path}`);
	}
	return response.json();
}

/** The id of the run GitHub says it created, from the answer to a dispatch; null when it does not say. */
export function runId(body) {
	try {
		const answer = JSON.parse(body);
		const found = /\/actions\/runs\/(\d+)/.exec(answer.html_url || answer.run_url || '');
		return String(answer.workflow_run_id || (found && found[1]) || '') || null;
	} catch {
		return null;
	}
}

/**
 * Is this run being held by GitHub with nothing in its way?
 *
 * Returns {held, why}. A held run is one GitHub has not got to yet: its status
 * is still "queued", as when it was created. That alone is not enough to ask
 * again, because a run may be waiting for its turn behind another run of the
 * workflow (the concurrency group in fetch.yml), and a second request would
 * only join that queue. Such a run reads "pending"; to be sure, a run that
 * reads "queued" while another run of the workflow is going is taken to be in
 * the queue too.
 *
 * Throws when GitHub cannot be asked: the caller then leaves things as they are.
 */
export async function held(env, id) {
	const run = await read(env, `actions/runs/${id}`);
	if (run.status === 'in_progress' || run.status === 'completed') {
		return { held: false, why: `it has started (${run.status})` };
	}
	if (run.status === 'pending') {
		return { held: false, why: 'it is waiting for its turn behind another run (pending)' };
	}
	if (run.status !== 'queued') {
		return { held: false, why: `its state reads "${run.status}", which is not one this knows as held` };
	}
	const going = await read(env, `actions/workflows/${env.WORKFLOW_FILE}/runs?status=in_progress&per_page=1`);
	if (going.total_count > 0) {
		return { held: false, why: 'another run is going, so it is waiting for its turn' };
	}
	return { held: true, why: 'it has not started and no other run is going' };
}

/** What to say in the log about the token's expiration date, and whether it is near. */
export function expiry(expires, now = Date.now()) {
	if (!expires) {
		return { text: 'the token has no expiration date', soon: false };
	}
	// GitHub writes "2027-11-15 10:59:13 UTC" (or with an offset such as "+0000").
	const when = Date.parse(expires.replace(' UTC', 'Z').replace(' ', 'T').replace(/ ([+-]\d{4})$/, '$1'));
	if (Number.isNaN(when)) {
		return { text: `the token expires ${expires}`, soon: false };
	}
	const days = Math.floor((when - now) / 86400000);
	return { text: `the token expires ${expires}, in ${days} days`, soon: days < WARN_DAYS };
}

/**
 * Ask GitHub to run the workflow. Returns what GitHub answered; throws, with a
 * message that says what to look at, when the run was not started. The token
 * is never part of a message or of a log line.
 */
export async function dispatch(env, wait = sleep, workflow = env.WORKFLOW_FILE, inputs = null) {
	for (const name of ['GITHUB_TOKEN', 'GITHUB_REPOSITORY', 'WORKFLOW_FILE', 'GIT_REF']) {
		if (!env[name]) {
			throw new Error(`${name} is not set: see scheduler/README.md`);
		}
	}

	let failure = 'not attempted';
	for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
		if (attempt > 1) {
			await wait(RETRY_AFTER_MS);
		}
		let answer;
		try {
			answer = await post(env, workflow, inputs);
		} catch (error) {
			failure = `could not reach GitHub (${error.message})`;
			continue;
		}
		if (answer.status >= 200 && answer.status < 300) {
			return answer;
		}
		failure = `GitHub answered HTTP ${answer.status}: ${answer.body}`;
		if (answer.status === 401) {
			failure += ' - the token is wrong, was revoked or has expired: make a new one (scheduler/README.md)';
		} else if (answer.status === 403 || answer.status === 404) {
			failure += ' - the token cannot start this workflow: it needs "Actions: Read and write" on this repository';
		}
		if (answer.status < 500) {
			break;                // the answer will not change on a retry
		}
	}
	throw new Error(`Workflow not started: ${failure}`);
}

/**
 * The workflows the site's status page may ask for, and whether each takes
 * the `force` input (run even if the data are fresh). Nothing else can be
 * started this way.
 */
export const ON_REQUEST = {
	'fetch.yml': { force: true },
	'fetch-capacity.yml': { force: false },
	'fetch-datacenters.yml': { force: false },
	'fetch-transition.yml': { force: false },
	'fetch-ember.yml': { force: true },
};

const answerJson = (status, body) => new Response(JSON.stringify(body), {
	status,
	headers: { 'Content-Type': 'application/json; charset=UTF-8', 'Cache-Control': 'no-store' },
});

/**
 * A run asked for by the site's Worker, through its service binding:
 *
 *   POST /dispatch  {"workflow": "fetch.yml", "force": true}
 *
 * Answers {started: true, status, run} or {started: false, error}. The site
 * checks the visitor's PIN session before it asks, and limits how often; this
 * side only checks that the workflow is one of ON_REQUEST. There is no second
 * look two minutes later, as there is for the schedule: a person is watching
 * and can press the button again.
 */
export async function onRequest(request, env) {
	const { pathname } = new URL(request.url);
	if (pathname !== '/dispatch') {
		return answerJson(404, { started: false, error: 'not found' });
	}
	if (request.method !== 'POST') {
		return answerJson(405, { started: false, error: 'use POST' });
	}
	let asked;
	try {
		asked = await request.json();
	} catch {
		return answerJson(400, { started: false, error: 'the body is not JSON' });
	}
	const workflow = asked && asked.workflow;
	const rules = Object.hasOwn(ON_REQUEST, workflow) ? ON_REQUEST[workflow] : null;
	if (!rules) {
		return answerJson(400, { started: false, error: 'unknown workflow', available: Object.keys(ON_REQUEST) });
	}
	const inputs = rules.force && asked.force === true ? { force: 'true' } : null;
	try {
		const answer = await dispatch(env, sleep, workflow, inputs);
		let run = '';
		try {
			run = JSON.parse(answer.body).html_url || '';
		} catch {
			// HTTP 204: started without saying which run.
		}
		console.log(`Started ${workflow} on ${env.GIT_REF} for the status page (HTTP ${answer.status}${inputs ? ', force' : ''}) ${run}`);
		return answerJson(200, { started: true, status: answer.status, run });
	} catch (error) {
		console.error(`${error.message} (${workflow}, asked for by the status page)`);
		return answerJson(502, { started: false, error: error.message });
	}
}

export default {
	async fetch(request, env) {
		return onRequest(request, env);
	},

	async scheduled(controller, env, ctx) {
		const due = new Date(controller.scheduledTime).toISOString();
		const what = `${env.WORKFLOW_FILE} on ${env.GIT_REF} of ${env.GITHUB_REPOSITORY}`;
		let id = null;
		try {
			const answer = await dispatch(env);
			let run = '';
			try {
				run = JSON.parse(answer.body).html_url || '';
			} catch {
				// An empty answer (HTTP 204): GitHub started the run without saying which.
			}
			id = runId(answer.body);
			const token = expiry(answer.expires);
			console.log(`Started ${what} (due ${due}, HTTP ${answer.status}) ${run}; ${token.text}`);
			if (token.soon) {
				console.warn(`Renew the GitHub token: ${token.text}. See scheduler/README.md`);
			}
		} catch (error) {
			console.error(`${error.message} (${what}, due ${due})`);
			throw error;          // so that the invocation shows as failed in the dashboard
		}
		await again(env, id, due);
	},
};

/**
 * Two minutes after a run was asked for, look at it and ask once more if
 * GitHub is holding it. Once only: this is called once for each run the
 * schedule asks for, and the second run is not looked at.
 *
 * The second request cannot put two runs side by side: the workflow's
 * concurrency group lets one run at a time. Nor does it cost requests to
 * ENTSO-E for nothing: when the held run is let go at last, it finds the data
 * the second one published and, if they are under 7 minutes old, skips
 * (skip_if_fresh.py). Whatever goes wrong here is logged and goes no further:
 * the run the schedule asked for was asked for.
 */
export async function again(env, id, due, wait = sleep) {
	if (!id) {
		console.warn(`GitHub did not say which run it created (due ${due}): it cannot be looked at, so it is not asked for again`);
		return 'unknown';
	}
	const seconds = Number(env.CHECK_AFTER_SECONDS);
	await wait((Number.isFinite(seconds) && seconds >= 0 && env.CHECK_AFTER_SECONDS !== '' ? seconds : CHECK_AFTER_S) * 1000);
	let state;
	try {
		state = await held(env, id);
	} catch (error) {
		console.error(`Could not look at run ${id} (due ${due}): ${error.message} - not asking again`);
		return 'not checked';
	}
	if (!state.held) {
		console.log(`Run ${id} (due ${due}) two minutes on: ${state.why} - not asking again`);
		return 'not held';
	}
	try {
		const answer = await dispatch(env, wait);
		const second = runId(answer.body);
		console.warn(`Run ${id} (due ${due}) two minutes on: ${state.why} - asked again`
			+ (second ? `: https://github.com/${env.GITHUB_REPOSITORY}/actions/runs/${second}` : ''));
		return 'asked again';
	} catch (error) {
		console.error(`Run ${id} (due ${due}) is held and asking again failed: ${error.message}`);
		return 'failed';
	}
}
