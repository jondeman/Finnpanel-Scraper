# Finnpanel Scraper — Technical Documentation

A scheduled scraper that captures the most-watched programmes on Finland's three
main streaming services (Yle Areena, MTV Katsomo, Ruutu) from
[Finnpanel](https://www.finnpanel.fi/), commits one CSV per period per day, and
serves the archive through a static dashboard.

Collecting since **2 September 2024**. Runs unattended on GitHub Actions.

> `README.md` contains a generated block between `<!-- BEGIN:generated -->` and
> `<!-- END:generated -->`. Everything outside those markers is preserved across
> runs and is safe to edit.

---

## Contents

- [How it works](#how-it-works)
- [Data schema](#data-schema)
- [The upstream source](#the-upstream-source)
- [Repository layout](#repository-layout)
- [The dashboard](#the-dashboard)
- [Running it yourself](#running-it-yourself)
- [Operations](#operations)
- [Design decisions](#design-decisions)
- [What changed in v2](#what-changed-in-v2)
- [Long-term durability risks](#long-term-durability-risks)

A chronological record of changes and the reasoning behind them is in
[DEVLOG.md](DEVLOG.md).

---

## How it works

```
 GitHub Actions cron (01:00 UTC daily)
              │
              ▼
      ┌───────────────┐   HTTP GET × 6, retried
      │   FPGH.py     │──────────────────────────►  finnpanel.fi
      └───────┬───────┘   (3 services × 2 periods)
              │
              │  parse <table class="totaltv">, capture the real measurement
              │  window, sort by Viewers, re-rank 1..60 across all services,
              │  validate 3 services × 20 rows
              ▼
   data/<year>/14D_Finnpanel_data_<date>.csv
   data/<year>/90D_Finnpanel_data_<date>.csv
              │
              ▼
      ┌────────────────────┐
      │ build_archive.py   │  folds the new days into
      └─────────┬──────────┘  data/finnpanel_all.csv
                │
                ▼
      ┌────────────────────┐
      │ update_readme.py   │  refreshes only the generated block
      └─────────┬──────────┘
                │
                ▼
      one commit, pushed to the branch the run started from (3 retries)
                │
                ▼
   index.html fetches from raw.githubusercontent.com at page load
                │
                ▼
      on failure: an issue is opened, labelled scraper-failure
```

Two datasets are produced each day:

| File prefix | Meaning | Source path |
|---|---|---|
| `14D_` | Each programme's most-watched episode over the **last 14 days** | `.../online14/3plus.html` |
| `90D_` | Same, over the **last 90 days** | `.../online90/3plus.html` |

Each file holds **60 rows** — the top 20 programmes from each of the three
services, pooled and re-ranked 1–60 by average viewers. `3plus` means the
audience panel is everyone aged 3 and over.

### Source files

| File | Role |
|---|---|
| [FPGH.py](FPGH.py) | Scrape and write CSVs. Writes files only — never touches GitHub. |
| [build_archive.py](build_archive.py) | Maintains `data/finnpanel_all.csv` |
| [update_readme.py](update_readme.py) | Rewrites only the generated block of `README.md` |
| [.github/workflows/Finnpanel_Scraper.yml](.github/workflows/Finnpanel_Scraper.yml) | Daily cron, commit/push, failure issue |
| [.github/workflows/jekyll-gh-pages.yml](.github/workflows/jekyll-gh-pages.yml) | Publishes the repo root to GitHub Pages |
| [index.html](index.html) | Client-side dashboard, reads CSV and legacy XLSX via SheetJS |
| [requirements.txt](requirements.txt) | Pinned dependencies, used by the workflow |

### Function reference — `FPGH.py`

| Function | Responsibility |
|---|---|
| `make_session()` | `requests.Session` with 4 retries, exponential backoff on 429/5xx, and a descriptive User-Agent. |
| `clean_int(value)` | `'1.'`→1, `'191 000'`→191000. Strips the ordinal dot and thousands separators; never treats a dot as a decimal point. |
| `parse_period(title)` | Extracts `Ajanjakso: 6.8.2026 – 19.8.2026` → `('2026-08-06', '2026-08-19')`. |
| `scrape_finnpanel(url, session)` | Scrapes one service/period. **Raises** on any failure rather than returning `[]`. |
| `process_data(...)` | Pools all three services, stable-sorts, re-ranks, validates. Returns `(DataFrame, problems)`. |
| `main()` | Writes both periods, exits non-zero if any problem was recorded. |

---

## Data schema

Per-day CSVs have nine columns:

| Column | Type | Example | Notes |
|---|---|---|---|
| `Date` | `YYYY-MM-DD` | `2026-08-21` | The **collection** date |
| `Rank` | int | `1` | 1–60, pooled across all three services. Not Finnpanel's per-service rank. |
| `Service` | string | `MTV Katsomo` | `MTV Katsomo`, `Ruutu`, `Yle Areena` |
| `Program` | string | `Salatut elämät` | Programme title |
| `Episode` | string | `Tohtori Hansen, otaksun?` | **Collected from 2026-08-21 onward.** Empty for Ruutu, which folds the episode into `Program` upstream. |
| `Duration` | `H:MM:SS` | `0:21:00` | Episode length, as text |
| `Viewers` | int | `191000` | Average viewers |
| `PeriodStart` | `YYYY-MM-DD` | `2026-08-06` | **From 2026-08-21 onward.** Start of the measured window. |
| `PeriodEnd` | `YYYY-MM-DD` | `2026-08-19` | **From 2026-08-21 onward.** End of the measured window. |

`data/finnpanel_all.csv` has the same columns plus **`Period`** (`14D`, `90D` or
`legacy`) in second position.

> ⚠️ **`Date` is not the period end.** The window typically closes one to two
> days before collection — a file dated 2026-08-21 describes 2026-08-06 …
> 2026-08-19. For time-series work use `PeriodEnd` where it exists. It cannot be
> reconstructed for rows collected before 2026-08-21.

**Archive as of 2026-08-21:** 86,088 rows, 719 distinct dates, 1,437 per-day files.

---

## The upstream source

Six URLs per run:

```
https://www.finnpanel.fi/tulokset/totaltv/{mtv,sanoma,yle}/online14/3plus.html
https://www.finnpanel.fi/tulokset/totaltv/{yle,mtv,sanoma}/online90/3plus.html
```

Each page contains a single `<table class="totaltv">`:

| row | cells | content |
|---|---|---|
| 0 | 1 | Title block: service, period label, **`Ajanjakso: 6.8.2026 – 19.8.2026`**, target group, population |
| 1 | 5 | Header: `#`, `Ohjelma`, `Jakso`, `Kesto`, `Keskikatsojamäärä` |
| 2–21 | 5 | 20 data rows |

The parser walks every row, skips anything with fewer than 5 cells (the title
block), and skips rows whose rank or viewer count will not parse (the header).
Row identity therefore does not depend on position, which makes it tolerant of an
extra header or footer row appearing upstream.

---

## Repository layout

```
Finnpanel-Scraper/
├── FPGH.py                    scraper
├── build_archive.py           consolidated archive builder
├── update_readme.py           README generated-block updater
├── index.html                 dashboard
├── requirements.txt           pinned deps
├── README.md                  partly generated
├── DOCUMENTATION.md           this file
├── STORAGE-PLAN.md            storage analysis and options
├── .gitattributes
├── .github/workflows/
└── data/
    ├── finnpanel_all.csv      the whole archive, one file (5.8 MB)
    ├── 2024/                  245 files
    ├── 2025/                  724 files
    └── 2026/                  468 files
```

Splitting by year keeps every directory well under GitHub's 1,000-entry
listing cap. At ~730 files a year, a year folder fills in about 16 months, so
this holds indefinitely.

### Coverage gaps

2024-09-02 → 2026-08-21, with **three days missing: 2025-08-24, 25 and 26**, in
both series. The workflow broke and was repaired by hand on 2025-08-27. Nothing
alerted anyone for three days — which is why v2 opens an issue on failure.

---

## The dashboard

[index.html](index.html) is a single self-contained page on GitHub Pages. It
loads SheetJS from a CDN and fetches data directly from
`raw.githubusercontent.com`, trying three paths in order:

1. `data/<year>/<range>_Finnpanel_data_<date>.csv` — current format
2. `data/<year>/<range>_Finnpanel_data_<date>.xlsx` — legacy, after the move
3. `<range>_Finnpanel_data_<date>.xlsx` — legacy, original root location

The third entry means **old permalinks keep resolving** even though the files
have moved. SheetJS parses both CSV and XLSX through the same call.

Reading from `raw.githubusercontent.com` rather than the published Pages bundle
is deliberate: see [Design decisions](#design-decisions).

---

## Running it yourself

No token and no write access required.

```bash
git clone https://github.com/jondeman/Finnpanel-Scraper.git
cd Finnpanel-Scraper
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python FPGH.py --dry-run     # scrape and print, write nothing
python FPGH.py               # write CSVs into data/<year>/
python build_archive.py      # fold them into the consolidated archive
```

Reading the archive:

```python
import pandas as pd
df = pd.read_csv('data/finnpanel_all.csv')

# Average audience by service, 14-day window
df[df.Period == '14D'].groupby('Service').Viewers.mean()

# One programme over time — use PeriodEnd, not Date
salkkarit = df[(df.Program == 'Salatut elämät') & (df.Period == '14D')]
salkkarit.set_index('PeriodEnd').Viewers.plot()
```

---

## Operations

### Trigger a run by hand
Actions → **Update Finnpanel Data** → *Run workflow*.

### When a run fails
An issue appears labelled `scraper-failure`. Subsequent failures comment on the
same issue rather than opening new ones. Close it once resolved so the next
failure is visible again.

### Backfill a missed day
**Not possible.** Finnpanel publishes only a rolling window. This is why the
scraper commits partial data and then fails the run, rather than discarding it —
40 rows beat none, and the red run tells you to look.

### Rebuild the archive from scratch
```bash
python build_archive.py --rebuild
```
Re-reads all 1,437 per-day files including the legacy `.xlsx` (~2 minutes). The
builder refuses to write an archive with fewer rows than the existing one.

### Expected timing
Cron is `0 1 * * *` (01:00 UTC). GitHub delays scheduled runs under load;
historically these land between 01:00 and 05:00 UTC. A late run is normal.

---

## Design decisions

**The scraper does not write to GitHub.** v1 committed through the PyGithub
Contents API, one commit per file. v2 writes plain files and lets the workflow
commit once. This removed a dependency, made the script runnable locally without
a token, and cut three commits a day to one.

**Partial data is committed, then the run fails.** The alternative — refusing to
write anything incomplete — throws away data that can never be recovered. Writing
it and going red preserves the data *and* raises the alarm.

**The dashboard reads `raw.githubusercontent.com`, not its own Pages bundle.**
GitHub deliberately does not trigger workflows from commits made with
`GITHUB_TOKEN`, so `jekyll-gh-pages.yml`'s `on: push` does not fire from the
daily run and the Pages bundle's *data* would be stale. Reading raw from `main`
sidesteps this entirely.
⚠️ The corollary: **edits to `index.html` do not go live automatically.** Run the
Pages workflow by hand after changing it.

**The workflow pushes to the branch it ran from, not to a hardcoded `main`.**
`git push origin HEAD:$GITHUB_REF_NAME` means a `workflow_dispatch` on a test
branch commits to that branch. Testing the real workflow end-to-end therefore
cannot touch production data.

**No personal access token is needed.** v1 required a `GT_TOKEN` secret for the
Contents API. v2 uses the automatic per-run `GITHUB_TOKEN` together with the
`permissions:` block, so there is no PAT to expire and silently kill the job.
The `GT_TOKEN` repository secret is now unused and can be removed once you have
confirmed nothing else reads it.

**Legacy `.xlsx` files were moved, not converted.** Converting them would keep
both copies in git history forever to save a few MB of working tree. They were
`git mv`'d — recorded as renames, costing almost nothing — and are represented in
open format inside `data/finnpanel_all.csv`. See [STORAGE-PLAN.md](STORAGE-PLAN.md).

---

## What changed in v2

Every item below was a real defect confirmed against the live site or the
archive.

| # | Was | Now |
|---|---|---|
| 1 | `README.md` truncated and rewritten daily, destroying any hand-written text | Only the `BEGIN:generated` block is replaced |
| 2 | `Episode` empty in **all 86,088 rows** — `len(cols) > 5` never true, rows have exactly 5 | Collected correctly |
| 3 | Real measurement window (`Ajanjakso`) discarded; `Date` used as if it were the period end | `PeriodStart` / `PeriodEnd` captured |
| 4 | Dashboard fetched the **wrong day** for every visitor west of UTC | Dates handled as strings; verified across three time zones |
| 5 | A service failing produced a silently-committed 40-row file, green run | Row and service counts validated; run fails and opens an issue |
| 6 | `requirements.txt` ignored; workflow hardcoded an unpinned list | Workflow installs from it; versions pinned |
| 7 | Bare `except:` treated auth and rate-limit errors as "file not found" | Upload path removed entirely |
| 8 | No timeout, no retry, no User-Agent | 4 retries with backoff, 30s timeout, descriptive UA |
| 9 | 1,440 entries in the root directory, past GitHub's 1,000-entry listing cap | Split into `data/<year>/` |
| 10 | Unstable sort churned the diff on tied viewer counts | Stable mergesort on `(Viewers, Service, Program)` |
| 11 | Module-level code ran on import | `if __name__ == '__main__'` |
| 12 | Python 3.10 (EOL October 2026) | Python 3.12 |
| 13 | No consolidated file; analysis meant globbing 1,435 spreadsheets | `data/finnpanel_all.csv` |
| 14 | No local run mode — required a write token | `--dry-run`, and normal runs write only local files |
| 15 | Required a `GT_TOKEN` PAT that would eventually expire | Uses the automatic `GITHUB_TOKEN`; no PAT at all |
| 16 | Workflow pushed to a hardcoded `main`, so it could not be safely test-run | Pushes to `$GITHUB_REF_NAME` |

**Data integrity was verified after the migration:** all 1,435 original `.xlsx`
files are present and byte-identical, git recorded 1,435 renames and zero
deletions, and 25 randomly sampled days reproduce exactly in the consolidated
archive.

**Verified on GitHub Actions**
([run 32470848610](https://github.com/jondeman/Finnpanel-Scraper/actions/runs/32470848610),
2026-08-21, 25s, all steps green). The **data files it produced were
byte-identical to the local run** — its commit contains nothing but the README
timestamp — confirming the scrape is deterministic across machines and Python
versions. See [DEVLOG.md](DEVLOG.md).

---

## Long-term durability risks

| Risk | Horizon | Detail | Mitigated? |
|---|---|---|---|
| **Scheduled workflows auto-disable** | 60 days idle | GitHub disables cron in public repos after 60 days without activity. A broken scraper stops commits, so the schedule switches off too. | Partly — the failure issue should get attention first |
| **Upstream markup change** | any time | Parser depends on `class="totaltv"` and on `Kesto`/`Keskikatsojamäärä` being the last two cells. | Yes — fails loudly instead of silently |
| **Action major versions deprecate** | 1–3 yrs | `checkout@v4`, `setup-python@v5`, `github-script@v7`. | No — review yearly |
| **Dependency breakage** | 1–3 yrs | Pinned to major versions; a `<3` ceiling on pandas eventually needs raising. | Partly |
| **Python 3.12 goes EOL** | ~2028 | Bump the workflow. | Deferred |
| **Repository size** | 100+ yrs | ~3.6 MB/yr as CSV. See [STORAGE-PLAN.md](STORAGE-PLAN.md). | Not a concern |

The one thing worth checking periodically: **that the daily run is still green.**
Everything else degrades slowly; a silent collection failure loses data
permanently, one day at a time.
