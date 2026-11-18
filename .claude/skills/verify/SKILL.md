---
name: verify
description: Confirm that a change to the feed works - the offline tests and validity checks, and, for a change to what the feed publishes, a real run of fetch.yml and a reading of the files it published. Use before calling the work done and before each commit that touches the fetchers, their tests, the workflows or scheduler/. Not for changes to documentation only.
---

# Verify a change to the feed

## When this applies

| The change touches | Run |
| --- | --- |
| A fetcher or a shared module (`*.py` at the root), or `tests/` | Part 1 |
| A workflow (`.github/workflows/*.yml`) | Part 1 |
| `scheduler/` (the Cloudflare Worker) | Part 1 with `--scheduler` |
| What the feed publishes in `data/`: a file's shape or values, or the steps of `fetch.yml` | Part 1, then part 2 after the push |
| The newer datasets: `fetch_balancing.py`, `fetch_outages.py`, `fetch_forecast.py`, `fetch_low_prices.py`, `wind_solar_actual.py`, `price_intervals.py`, or what `feed_common.py` gives them | Part 1, then part 2 after the push |
| Documentation only (`*.md`, comments) | Nothing here |

## Part 1: offline, under two minutes

```bash
.claude/skills/verify/offline.sh
```

Add `--scheduler` when the change touches `scheduler/` (it is added by itself when git shows changes there). It runs, without network, and stops at the first failure:

1. Every Python file compiles.
2. The tests of `tests/` (`python3 -m unittest discover -s tests`).
3. The Worker's tests (`npm test` in `scheduler/`), when asked for.
4. Every workflow is valid YAML with a trigger and a job.
5. Every JSON file under `data/` parses, the archive included.

It prints one line for each check and `OFFLINE OK` at the end. Anything else is a failure of the change: fix the code, not the test.

The tests of the newer datasets run on real answers of ENTSO-E and real files of OMIE, kept in `tests/fixtures/entsoe` and `tests/fixtures/omie` (the larger ones gzipped). A change to how an answer is read is checked against them; a new kind of answer needs a new sample, taken on GitHub, where the key is.

## Part 2: a real run, for what the feed publishes

The ENTSO-E key is only on GitHub, so what a change does to the published files can only be seen in a run of `fetch.yml`. After the push:

```bash
GH_TOKEN=$(gh auth token --user joaopasbento) python3 .claude/skills/verify/real_run.py
```

It waits (25 minutes at most) for the first run of `fetch.yml` that started from the pushed commit and actually fetched (a run skipped because the data were fresh does not count), reads the files that run published from `origin/main`, and prints what it found. It ends in one of three ways:

- `REAL RUN OK` (exit 0): the run published, and the files have the shape the site reads.
- `REAL RUN BROKEN` (exit 1): a file is not valid, lost a field the site reads, or the run failed with ENTSO-E answering. This is the change: fix or revert it.
- `REAL RUN INCONCLUSIVE` (exit 2): a source was down. ENTSO-E gave nothing for a file in that run, so the file was left as it was and says nothing about the change. Run the script again for the next run; do not count it against the change.

A source being down is told from a broken change by the feed's own rule for kept values: an entry the source did not answer for is published with `carried_over`, and a file none of whose entries were obtained is not rewritten (its `updated` stays older than the run). The script lists the kept entries as information, never as a failure.

### The newer datasets

A run has two halves. The steps that make the three files above come first and are committed and pushed; then come the steps of the newer datasets, each with its own time budget and `continue-on-error`, and a second commit publishes their files. The script reads those too, from `origin/main`, once the run has ended:

| File | Made by | What must hold |
| --- | --- | --- |
| `data/balancing-prices.json` | `fetch_balancing.py` | blocks `afrr_activated` (`cc-by-4.0`) and `afrr_cbmp` (`source-terms`); each day of a zone has one value for each period |
| `data/outages.json` | `fetch_outages.py` | blocks `generation` (`source-terms`) and `transmission` (`cc-by-4.0`); every outage has its fields |
| `data/wind-solar-forecast.json` | `fetch_forecast.py` | block `wind_solar` (`source-terms`); each day has forecast, actual and error for each hour |
| `data/low-prices.json` | `fetch_low_prices.py` | blocks `omie` (`omie`) and `entsoe` (`source-terms`); the total of the year for Portugal and Spain |
| `data/wind-solar-actual.json`, `data/spot-prices-intervals.json` | `fetch_entsoe.py`, for the two above | one block each, `source-terms` |

Every one of them must have `updated`, `data_time` and the licence of each block: a file that lost one of these, or its shape, is `REAL RUN BROKEN`. A file that was not written in the run is only a note ("not written in this run"): its step found nothing new or its source did not answer, and the file stays as it was by the rule above. A file that is not there at all is a note too, the first time its step runs.

Two more things are read off the run itself: the result of each of the newer steps, and how long the whole job took. The job must fit in the cadence of 15 minutes; over 13 it is `REAL RUN BROKEN`, whatever the files say.

The records of the archive (`data/archive/outages-*.json`, `wind-solar-forecast-*.json`, `low-prices-*.json`) have no time of their own and only change when their numbers do: they are checked by part 1 (they parse) and by the tests.

The script checks the shape that every change must leave intact. What a particular change is meant to do (a new field, a pair that should now appear, a value that should match another source) is checked by reading the published file for exactly that, from `origin/main`:

```bash
git fetch -q && git show origin/main:data/cross-border-flows.json | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['net']))"
```

Say in the report which run was read and what was seen in it.
