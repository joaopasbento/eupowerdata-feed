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
 * It has no address of its own (no fetch handler, no workers.dev route): the
 * only thing that starts it is its Cron Trigger.
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** One request to GitHub. Returns {status, body}; throws on a network error. */
async function post(env) {
	const url = `${API}/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${env.WORKFLOW_FILE}/dispatches`;
	const response = await fetch(url, {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${env.GITHUB_TOKEN}`,
			Accept: 'application/vnd.github+json',
			'Content-Type': 'application/json',
			'X-GitHub-Api-Version': API_VERSION,
			'User-Agent': USER_AGENT,
		},
		body: JSON.stringify({ ref: env.GIT_REF }),
	});
	return { status: response.status, body: (await response.text()).slice(0, 500) };
}

/**
 * Ask GitHub to run the workflow. Returns what GitHub answered; throws, with a
 * message that says what to look at, when the run was not started. The token
 * is never part of a message or of a log line.
 */
export async function dispatch(env, wait = sleep) {
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
			answer = await post(env);
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

export default {
	async scheduled(controller, env, ctx) {
		const due = new Date(controller.scheduledTime).toISOString();
		const what = `${env.WORKFLOW_FILE} on ${env.GIT_REF} of ${env.GITHUB_REPOSITORY}`;
		try {
			const answer = await dispatch(env);
			let run = '';
			try {
				run = JSON.parse(answer.body).html_url || '';
			} catch {
				// An empty answer (HTTP 204): GitHub started the run without saying which.
			}
			console.log(`Started ${what} (due ${due}, HTTP ${answer.status}) ${run}`.trim());
		} catch (error) {
			console.error(`${error.message} (${what}, due ${due})`);
			throw error;          // so that the invocation shows as failed in the dashboard
		}
	},
};
