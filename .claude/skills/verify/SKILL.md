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
| The newer datasets: `fetch_balancing.py`, `fetch_outages.py`, `fetch_forecast.py`, `fetch_low_prices.py`, `fetch_open_prices.py`, `wind_solar_actual.py`, `price_intervals.py`, or what `feed_common.py` gives them | Part 1, then part 2 after the push |
| The exchange rates or what is converted with them (`fx_rates.py`), Ember's figures and the capacity file (`fetch_ember.py`, `merge_capacity.py`) | Part 1, then part 2 after the push |
| The state of the files for the site's status page (`build_meta.py`, `data/meta.json`) | Part 1, then part 2 after the push |
| The price history of the Pro plan (`fetch_price_history.py`, `price-history.yml`, `fill-price-history.yml`) | Part 1, then the reading of the history after a run of one of those workflows (end of part 2) |
| The records and the PPA index (`fetch_records.py`, `records.yml`, `fill-records.yml`, `fetch_ppa_index.py`) | Part 1, then the reading of the records after a run of one of those workflows (end of part 2) |
| How the negative, zero and low prices are counted, or a rewrite of their daily records | Part 1, part 2 after the push, and the proof of part 3 |
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

The tests of the newer datasets run on real answers of ENTSO-E and real files of OMIE, kept in `tests/fixtures/entsoe` and `tests/fixtures/omie` (the larger ones gzipped), and those of the open prices on real answers of SMARD, Elexon and Energy-Charts, in `tests/fixtures/smard` (gzipped), `tests/fixtures/elexon` and `tests/fixtures/energy_charts`. A change to how an answer is read is checked against them; a new kind of answer needs a new sample, taken on GitHub for ENTSO-E, where the key is (SMARD, Elexon and Energy-Charts need no key; Energy-Charts' /price allows only 2 requests a minute).

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
| `data/balancing-prices.json` | `fetch_balancing.py` | blocks `afrr_activated` (`cc-by-4.0`) and `afrr_cbmp` (`source-terms`); each day of a zone has one value for each period; a zone not in euros has, in each day, `up_eur` and `down_eur` of the same length, with `fx_rate` and `fx_date` (a day with no rate is a note); a zone asked for by more than one area says which in `area`; the note gives Italy's area, its process (the one with more periods priced, A16 when they tie) and how many periods priced it has |
| `data/outages/index.json` and the file of each zone it lists, `data/outages/<ZONE>.json` | `fetch_outages.py` | blocks `generation` (`source-terms`) and `transmission` (`cc-by-4.0`) in the index and in each file; every outage has its fields and none is cancelled or withdrawn; each file has `unavailable_now` (MW and units, each unit counted once) and no time of the run, and the index says how many outages each file has; `data/outages.json` is no longer published. The note gives what is out now in Spain and Portugal, with the outages in force added up beside it |
| `data/wind-solar-forecast.json` | `fetch_forecast.py` | block `wind_solar` (`source-terms`); each day has forecast, actual and error for each hour |
| `data/forecast/index.json` and the file of each zone it lists, `data/forecast/<ZONE>.json` | `fetch_forecast.py` | block `wind_solar` (`source-terms`); `days` of the index is the day in course and the 30 before it; each file has its days in order, within them, each with forecast, actual and error for each hour; the note gives how many days Spain and Portugal have |
| `data/low-prices.json` | `fetch_low_prices.py` | blocks `omie` (`omie`) and `entsoe` (`source-terms`); the total of the year for Portugal and Spain; each zone of `entsoe` has its `months`, which add up to its year; the note says when OMIE's later versions were last looked for (`omie.version_check`) and which zones still have days of the year counted on hourly means |
| `data/open-prices.json` | `fetch_open_prices.py` | the 18 zones of the paid alerts and no other, each with its own licence: the 15 of SMARD `cc-by-4.0` with the attribution `Bundesnetzagentur \| SMARD.de` (followed by `, energy-charts.info` while one of its days came from there), each of their days with its `source` (`smard` or `energy-charts`) and licence `cc-by-4.0`, a day of Energy-Charts with the `license_info` it was given under, which must say CC BY 4.0, GB `elexon` with the pounds and the exchange rate beside the euros, PT and ES `omie`; each zone has today and, once it is out, tomorrow (no other day), each day with one price for each interval from its start, none missing but in GB (whose half-hours still to come are null); the note says which zones have tomorrow, which days came from Energy-Charts, which zones were kept and which are not in the file |
| `data/wind-solar-actual.json`, `data/spot-prices-intervals.json` | `fetch_entsoe.py`, for the two above | one block each, `source-terms` |

Every one of them must have `updated`, `data_time` and the licence of each block: a file that lost one of these, or its shape, is `REAL RUN BROKEN`. A file that was not written in the run is only a note ("not written in this run"): its step found nothing new or its source did not answer, and the file stays as it was by the rule above. A file that is not there at all is a note too, the first time its step runs.

Three files are read by a run, or rewritten by it only now and then, so their time says nothing and only their shape is checked. A missing or misshapen one is `REAL RUN BROKEN`:

| File | Made by | What must hold |
| --- | --- | --- |
| `data/fx-rates.json` | `fx_rates.py`, called by the balancing step | `rates` with a day for each currency (the zloty and the leu among them), `fixed` with the lev and the Bosnian mark |
| `data/ember-yearly.json` | `fetch_ember.py`, in `fetch-ember.yml` (weekly) | `license` `cc-by-4.0`, `source`, `edition`, and the 32 countries, each with its share of renewables and a trend of seven years |
| `data/renewablenodes-capacity.json` | `merge_capacity.py`, which makes no request | the four top-level keys and, in each country, the 17 fields the site reads, in their order; `cap_source` is `entsoe` or `ember`; `source` names Ember |

Two more things are read off the run itself: the result of each of the newer steps, and how long the whole job took. The job must fit in the cadence of 15 minutes; over 13 it is `REAL RUN BROKEN`, whatever the files say.

The files of the zones (`data/outages/<ZONE>.json`, `data/forecast/<ZONE>.json`) are only written when what they say changes: one not written in a run is not a note. The records of the archive (`data/archive/outages-*.json`, `wind-solar-forecast-*.json`, `low-prices-*.json`) have no time of their own and only change when their numbers do: they are checked by part 1 (they parse) and by the tests.

The script checks the shape that every change must leave intact. What a particular change is meant to do (a new field, a pair that should now appear, a value that should match another source) is checked by reading the published file for exactly that, from `origin/main`:

```bash
git fetch -q && git show origin/main:data/cross-border-flows.json | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['net']))"
```

Say in the report which run was read and what was seen in it.

### The price history

`data/price-history/` is written by its own workflows, not by `fetch.yml`: `price-history.yml` once a day and `fill-price-history.yml` by hand (the first fill). After a run of either, read what it published:

```bash
python3 .claude/skills/verify/real_run.py --price-history
```

It ends `REAL RUN OK` or `REAL RUN BROKEN`. What must hold: `index.json` lists only the 17 zones of the history (the 15 of SMARD, `cc-by-4.0` with the attribution `Bundesnetzagentur \| SMARD.de`, and PT and ES, `omie` with the attribution `OMIE`), each with its first day and years, and the address of its licence (`license_url`: the CC BY 4.0 page, or OMIE's legal warning); `monthly.json` is there; and no Pro file (`data/price-history/<ZONE>/`, `data/price-history/daily/`) is in the repository: since 26 Nov 2026 they are in the private R2 bucket `eupowerdata-pro`, and the runs of `price-history.yml` and `fill-price-history.yml` end in error unless every one of them is in R2 as the run left it (`r2_price_history.sh check`). The note gives, for each zone, its first and last day, its sources in spans and how many days are missing. The same reading is a line of every real run of part 2 (a note while the history has not been filled). The tests run on real answers of SMARD, Energy-Charts and OMIE in `tests/fixtures/price_history`, `tests/fixtures/smard` and `tests/fixtures/energy_charts`.

### The records and the PPA index

`data/records.json` is written by `records.yml` (daily) and `fill-records.yml` (by hand: the one-off fill of the generation since 2021, or, with `sample`, real answers of ENTSO-E for the tests as an artifact); `data/ppa-index.json` by a step of `price-history.yml`. After a run, read both:

```bash
python3 .claude/skills/verify/real_run.py --records
```

What must hold: the thirteen types with their stable names (`solar_mw_max`, `wind_mw_max`, `renewable_share_interval_max`, `renewable_share_day_max`, `price_interval_min`, `price_day_avg_min`, `negative_zero_hours_day_max`, `solar_gwh_day_max`, `wind_gwh_day_max`, `negative_zero_run_hours_max` by zone, licence `source-terms`, a run with its `end` after its start; each type with its `name`; `solar_share_month_max`, `wind_share_month_max`, `renewable_share_month_max` by country, `cc-by-4.0`), each record with its value, unit, moment, source and `previous`, and `recent` the most recent first; the PPA index has a point a quarter from 2023-01, each with its trading day, `FTB PPA` contract and price. The tests run on real answers in `tests/fixtures/records` (ENTSO-E's A75 of whole days, the clock changes among them, the archives' prices and Ember's monthly rows) and `tests/fixtures/ppa` (OMIP's pages).

## Part 3: the proof of the low prices

Portugal and Spain are counted twice in the daily records of the archive (`data/archive/low-prices-YYYY-MM-DD.json`): from OMIE's own files (block `omie`, day-ahead) and from the feed's prices of ENTSO-E (block `entsoe`). It is the same market at the same resolution, so the negative, zero and low hours must be equal day by day. After a change to how either is counted, and after a run of `rewrite-archive.yml` with `what: low-prices`, pull and run, with no network:

```bash
python3 .claude/skills/verify/low_prices_proof.py
```

It prints, for each of the two zones, the totals of each block over the days that have both, and every day that differs; it ends `PROOF OK` (exit 0) or `PROOF FAILED` (exit 1). On 18 November 2026 OMIE's totals of the year, from 1 January, were 550.25 negative, 361 zero and 929.75 low hours for Portugal and 697.5, 364.75 and 783 for Spain: the `entsoe` block must give the same over the same days.

A day that differs is reported with its numbers, never put right by hand: one count is not adjusted to the other. A day counted on hourly means (`basis` other than `market-resolution`) differs by construction; it is counted again by the rewrite.
