# Feed scheduler

A Cloudflare Worker that starts a run of the feed's workflow
(`.github/workflows/fetch.yml`, on `main`) every 15 minutes, at minutes 7, 22,
37 and 52 of every hour (UTC).

## Why

GitHub's own schedule drops most of its runs. Between 22:33 UTC on 15 November
2026 and 07:48 on the 16th it started one run, of the 37 that were due. The
Worker is the clock instead: on each Cron Trigger it asks GitHub to run the
workflow, through the
[workflow dispatch](https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event)
endpoint. The schedule in `fetch.yml` stays as a fallback, and the workflow's
`concurrency` group makes a run wait for the one in progress, so two never run
at once.

## What is here

| File | What it is |
| --- | --- |
| `src/index.js` | The Worker: a `scheduled` handler, and a `fetch` handler reached only through a service binding (no address of its own) |
| `wrangler.jsonc` | Its name, the Cron Trigger and what to start (repository, workflow, branch) |
| `test/dispatch.test.js` | Tests, without network: `npm test` in this folder |
| `waits.py` | Prints how long GitHub held each run before starting it (see below) |

Nothing secret is in this folder or anywhere in the repository.

## The GitHub token

The Worker needs a GitHub token to start the workflow. It is a secret of the
Worker, named `GITHUB_TOKEN`, set in the Cloudflare dashboard (Workers & Pages >
`eupowerdata-feed-scheduler` > Settings > Variables and Secrets). Once saved,
its value cannot be read back, there or with Wrangler.

It is a fine-grained personal access token with the least it can have:

- Repository access: **Only select repositories** > `joaopasbento/eupowerdata-feed`
- Repository permissions: **Actions: Read and write** (GitHub adds
  Metadata: Read-only by itself). Nothing else: in particular not Contents, so
  the token cannot read or change the code or the data.

With it, someone could start, cancel and re-run this repository's workflows.
They could not change what the workflows do.

### When it expires

Every line the Worker logs for a run it started ends with the date GitHub says
the token expires on (`... the token expires 2027-11-15 10:59:13 UTC, in 363
days`), and in the last two weeks it adds a warning line (`Renew the GitHub
token ...`).

GitHub revokes the token on its expiration date. From then on GitHub answers
HTTP 401 and the Worker starts nothing: each invocation fails, with the reason
in its log (`Workflow not started: GitHub answered HTTP 401 ... has expired`).
The feed does not stop, it falls back on GitHub's own schedule, which runs a
few times a day.

To renew it: create a new token with the same access and permission, then in
the Cloudflare dashboard open the Worker > Settings > Variables and Secrets,
edit `GITHUB_TOKEN`, paste the new value and deploy. Nothing changes in this
repository.

## Runs asked for by the site's status page

The site's own Worker (`eupowerdata-site`) has a status page behind a PIN, with
buttons that ask for a run now. It does not hold a GitHub token: it asks this
Worker, through a [service binding](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/),
with `POST /dispatch {"workflow": "fetch.yml", "force": true}`, and this Worker
starts the run with its own `GITHUB_TOKEN`. A service binding is a call from
one Worker to another inside Cloudflare: it cannot be reached from the
internet, and this Worker still has no workers.dev address and no route.

Only the workflows in `ON_REQUEST` (`src/index.js`) can be started this way:
`fetch.yml` and `fetch-ember.yml` (with `force` when asked), and
`fetch-capacity.yml`, `fetch-datacenters.yml` and `fetch-transition.yml`. The
site checks the PIN session and how often the buttons are pressed before it
asks. Each run started this way has its own log line
(`Started fetch.yml on main for the status page ...`).

## Deploying

After a change to this folder, from the folder itself:

```
npm test
npx wrangler deploy
```

`wrangler deploy` needs a Wrangler signed in to the Cloudflare account
(`npx wrangler login`, in the browser). It replaces the code, the Cron Trigger
and the three plain settings; it leaves the `GITHUB_TOKEN` secret as it is.

## Checking that it works

- GitHub: the [runs of the workflow](https://github.com/joaopasbento/eupowerdata-feed/actions/workflows/fetch.yml)
  have one run, started by `workflow_dispatch`, a few seconds after minutes 7,
  22, 37 and 52.
- Cloudflare: Workers & Pages > `eupowerdata-feed-scheduler` > Logs has one line
  for each run started (`Started fetch.yml on main ...`), or the reason it
  could not be.

## When GitHub holds a run

GitHub creates a run the moment the Worker asks for it, but now and then
leaves it for a long time before starting its job, with no other run in the
way: 5 of the 85 runs of 16 and 17 November 2026, for 14 to 65 minutes. The
wait is not for a machine (one is assigned within milliseconds of GitHub
getting to the run), and an experiment over 74 runs found it had nothing to do
with the version of the API the request is sent with, nor with GitHub's own
schedule having a run due in the same minute. A request made while a run is
held starts at once all the same.

So the Worker asks again. Two minutes after asking for a run it looks at it
and, if it has not started and no other run of the workflow is going, asks for
one more, once. It does this inside the same invocation, which waits the two
minutes (`CHECK_AFTER_SECONDS` in `wrangler.jsonc`); a Worker started by a
Cron Trigger may run for 15 minutes and waiting takes none of its 10 ms of CPU
time. Nothing is stored and there is no second trigger.

- A run that is waiting its turn behind another is not held, and is not asked
  for again.
- If GitHub cannot be asked about the run, nothing is asked for again; the log
  says why.
- The second run cannot go side by side with another: `fetch.yml` lets one run
  at a time. When the held run is let go at last, it finds the data the second
  one published and skips if they are under 7 minutes old (`skip_if_fresh.py`).

The log has one more line for each turn: `Run <id> (due ...) two minutes on:
it has started (in_progress) - not asking again`, or `... it has not started
and no other run is going - asked again: <the second run>`.

`waits.py` reads the runs through the API and prints the wait of each one,
split into the time spent behind another run and the time held for no reason:

```
GH_TOKEN=$(gh auth token --user joaopasbento) python3 scheduler/waits.py
```

## Limits of the free plan

Checked in Cloudflare's documentation on 16 November 2026
([limits](https://developers.cloudflare.com/workers/platform/limits/),
[pricing](https://developers.cloudflare.com/workers/platform/pricing/)):

| | Free plan | This Worker |
| --- | --- | --- |
| Requests | 100,000 a day | 96 a day |
| Cron Triggers | 5 in the account | 1 |
| CPU time for each Cron Trigger | 10 ms | about 1 ms (two to four small requests) |
| Requests it can make each time | 50 | 2, or 4 when it asks again |
| How long one invocation may last | 15 minutes | 2 minutes, nearly all of it waiting |
| Log lines kept | 200,000 a day, for 3 days | 96 a day |

On GitHub's side, a personal access token can make 5,000 API requests an hour,
and the workflow runs on a public repository, where Actions minutes are free.
