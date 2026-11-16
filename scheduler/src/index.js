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
const OLD_API_VERSION = '2022-11-28';
const USER_AGENT = 'EUPowerData-Feed-Scheduler/1.0 (+https://eupowerdata.com)';
const ATTEMPTS = 2;           // one retry, after a network error or an HTTP 5xx
const RETRY_AFTER_MS = 3000;
const WARN_DAYS = 14;         // how long before the token expires the log starts warning

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * The experiment of 16 Nov 2026 (on while the EXPERIMENT setting is "on").
 *
 * GitHub creates every run the moment it is asked for, but left three of the
 * first eleven sitting for 14, 20 and 26 minutes before starting them, with no
 * other run in the way. Two things this scheduler can vary might be behind it,
 * and each is varied on half of the four runs of every hour:
 *
 *   reserve / alone    whether GitHub's own schedule, kept as a fallback in
 *                      fetch.yml, has a run due in this same minute. Its cron
 *                      lines cover minutes 7 and 37 of the even hours and
 *                      minutes 22 and 52 of the odd ones (UTC); this function
 *                      only works out which case a run falls in.
 *   api-new / api-old  the X-GitHub-Api-Version the request is sent with:
 *                      2026-03-10 or 2022-11-28. The old one goes out in the
 *                      second half of the hour, and the half swaps every two
 *                      hours.
 *
 * Every hour has the four combinations once, and over four hours each one
 * falls on each of the four minutes, so neither the minute nor the hour can
 * pass for an effect. The run carries its combination in its title (the
 * `variant` input of the workflow), where scheduler/waits.py reads it.
 */
export function variant(scheduledTime) {
	const due = new Date(scheduledTime);
	const hour = due.getUTCHours();
	const quarter = Math.floor(due.getUTCMinutes() / 15);        // 0 to 3: minutes 7, 22, 37, 52
	const reserve = (quarter % 2 === 0) === (hour % 2 === 0);
	const oldApi = (quarter >= 2) !== (Math.floor(hour / 2) % 2 === 1);
	return {
		apiVersion: oldApi ? OLD_API_VERSION : API_VERSION,
		label: `${oldApi ? 'api-old' : 'api-new'} ${reserve ? 'reserve' : 'alone'}`,
	};
}

/**
 * One request to GitHub. Returns {status, body, expires}; throws on a network
 * error. `expires` is when GitHub says the token stops working (it sends that
 * with every answer to a token that has an expiration date), or null.
 * `how` is the variant of the experiment, when it is on.
 */
async function post(env, how) {
	const url = `${API}/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${env.WORKFLOW_FILE}/dispatches`;
	const response = await fetch(url, {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${env.GITHUB_TOKEN}`,
			Accept: 'application/vnd.github+json',
			'Content-Type': 'application/json',
			'X-GitHub-Api-Version': how ? how.apiVersion : API_VERSION,
			'User-Agent': USER_AGENT,
		},
		body: JSON.stringify(how ? { ref: env.GIT_REF, inputs: { variant: how.label } } : { ref: env.GIT_REF }),
	});
	return {
		status: response.status,
		body: (await response.text()).slice(0, 500),
		expires: response.headers.get('github-authentication-token-expiration'),
	};
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
export async function dispatch(env, wait = sleep, how = null) {
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
			answer = await post(env, how);
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
		const how = env.EXPERIMENT === 'on' ? variant(controller.scheduledTime) : null;
		const what = `${env.WORKFLOW_FILE} on ${env.GIT_REF} of ${env.GITHUB_REPOSITORY}`
			+ (how ? ` [${how.label}, API ${how.apiVersion}]` : '');
		try {
			const answer = await dispatch(env, sleep, how);
			let run = '';
			try {
				run = JSON.parse(answer.body).html_url || '';
			} catch {
				// An empty answer (HTTP 204): GitHub started the run without saying which.
			}
			const token = expiry(answer.expires);
			console.log(`Started ${what} (due ${due}, HTTP ${answer.status}) ${run}; ${token.text}`);
			if (token.soon) {
				console.warn(`Renew the GitHub token: ${token.text}. See scheduler/README.md`);
			}
		} catch (error) {
			console.error(`${error.message} (${what}, due ${due})`);
			throw error;          // so that the invocation shows as failed in the dashboard
		}
	},
};
