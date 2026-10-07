# Devlog

Chronological record of substantive changes: what was done, what was measured,
and why each decision went the way it did. Newest first.

---

## 2026-10-07 — Demo breakdown (kohderyhmät)

Branch `main` (v1) · commit [`3f277df`](../../commit/3f277df) · production
[run 37664453001](https://github.com/jondeman/Finnpanel-Scraper/actions/runs/37664453001)

### Why

The scraper only fetched Finnpanel's `3plus` pages, the whole 3+ population, so
the dashboard could not show what any narrower audience watches.

### Research

`www.finnpanel.fi` is blocked by the development environment's network policy,
so the site structure was established from search-engine-indexed pages rather
than the live HTML. Every TotalTV top list follows one URL scheme:

`/tulokset/totaltv/{yle|mtv|sanoma}/{period}/{demo}.html`

Four target groups exist:

| Page | Finnpanel's name | `Demo` value |
|---|---|---|
| `3plus` | Kaikki 3 vuotta täyttäneet | `3+` |
| `alle45` | Alle 45-vuotiaat | `Alle 45` |
| `45plus` | 45 vuotta täyttäneet | `45+` |
| `ikar2564` | 25–64-vuotiaat | `25-64` |

Periods found: `online14` and `online90` (collected), plus `online28`, `total14`
and `total28` (not collected; `total*` include broadcast viewing).

Not being able to read the live markup left one real risk: that the demo pages
use a different table than the 3+ page. The production run settled it (below).

### Decisions worth recording

**One file per period with a `Demo` column**, not a file or sheet per demo. v1
commits each file through the Contents API, one commit per file, so per-demo
files would turn two data commits a day into eight. Keeping the filenames means
the dashboard URL scheme and two years of history work untouched, and one table
stays filterable and pivotable in Excel. `Demo` sits after `Date`; anything that
reads these files by column position sees the shift.

**Rank is per demo**: programmes are re-ranked across the three services within
each demo, the same way the 3+ ranking already worked.

**Files without a `Demo` column are 3+.** The dashboard offers only the demos
present in the loaded file and says so on older dates, rather than offering
filters that come back empty.

**A missing demo page is logged and skipped**, following v1's existing
behaviour. This is deliberately *not* v2's rule (write partial data, then fail
the run); see follow-ups.

### Verification

| Check | Result |
|---|---|
| Scraper against mocked pages | 12 fetches (4 demos × 3 services) per period; ranks 1..n within each demo; a page with no table skipped without aborting |
| Dashboard in headless Chromium with SheetJS 0.18.5 | New-format and legacy files: demo filter, compare view, demo × service, choice kept across period change, legacy note |
| Production run (manual dispatch on `main`) | Green in 46s; 240 rows per file, 20 per service in every demo, both periods |

Every demo came back with its own top list, which is what confirms the pages
parse rather than repeating 3+:

| 14D #1 | Programme | Viewers |
|---|---|---|
| 3+ | Salatut elämät (MTV Katsomo) | 201,000 |
| Alle 45 | Erikoisjoukot (Ruutu) | 118,000 |
| 45+ | Cooper ja Fry (Yle Areena) | 134,000 |
| 25-64 | Salatut elämät (MTV Katsomo) | 142,000 |

The run replaced that morning's 3+-only files for 2026-10-07
([`c3803e8`](../../commit/c3803e8), [`51deb5c`](../../commit/51deb5c)); the 3+
rows are unchanged inside them.

### Outcome

Rows per file 60 → 240. File size ~8.1 KB → ~15.5–15.9 KB (2026-10-06 vs
2026-10-07). Demo data begins 2026-10-07.

### Follow-ups

- [ ] **Port demos into `enhancements` before merging it.** That branch still
      scrapes only `3plus` and rewrote both `FPGH.py` and `index.html`, so the
      merge will conflict in both. When porting, append `Demo` at the end of
      `COLUMNS` (v2's rule, so old and new files concatenate) and make a missing
      demo fail the run after writing, per v2's partial-data rule.
- [ ] Decide whether `online28` and `total14`/`total28` are worth collecting.
- [ ] The dashboard table overflows at phone widths (pre-existing; one more
      column now).

### Known limitations

- Demo data exists only from 2026-10-07. History cannot be repaired, since
  Finnpanel publishes only a rolling window.
- On v1, a demo that stops parsing drops out of the file with only a log
  warning; the run stays green.
- `Alle 45` is labelled as Finnpanel names it. Within the 3+ universe it should
  mean ages 3–44; not confirmed against Finnpanel's methodology.

---

## 2026-08-21 — v2: rebuild for long-term unattended operation

Branch `enhancements` · commits [`70137a6`](../../commit/70137a6),
[`78d6a18`](../../commit/78d6a18) · CI
[run 32470848610](https://github.com/jondeman/Finnpanel-Scraper/actions/runs/32470848610)

### Why

The trigger was a cosmetic complaint — GitHub's file browser had started
truncating the repository root ("440 entries were omitted") — and the question
of whether the archive would eventually outgrow GitHub if the scraper ran for
years.

Investigating that turned up three problems that mattered considerably more than
storage: the scraper had been **discarding data it successfully downloaded**, it
was **recording the wrong dates**, and it could **fail silently for days**.

### Findings

**Storage was never the problem.** Measured, not estimated:

| | |
|---|---|
| Working tree (1,435 `.xlsx`) | 10.98 MB |
| Git pack | 8.12 MiB |
| Full clone | ~20 MB |
| Growth | ~9.7 MB/yr |
| **Reaches GitHub's 1 GB guidance** | **~year 2130** |

The real limit already being hit was the **1,000-entry directory listing cap**,
which is a layout problem, not a size problem.

**`Episode` was empty in all 86,088 archived rows.** The guard at `FPGH.py:51`
read `len(cols) > 5`, but Finnpanel's rows contain exactly 5 cells, so the branch
never executed. Confirmed against the live page: `Jakso` is populated (today's
MTV row 1 is *"Tohtori Hansen, otaksun?"*). **Two years of episode titles were
downloaded and thrown away.** Unrecoverable — Finnpanel publishes only a rolling
window.

**Dates were wrong by one to two days.** The table's title block carries
`Ajanjakso: 6.8.2026 – 19.8.2026`, the window the figures actually describe. It
was discarded in favour of `datetime.now()`. The dashboard then compounded the
error by *inferring* the window as `selected date − 13`, displaying
`2026-08-08 – 2026-08-21` for a file covering `2026-08-06 – 2026-08-19`.

**Failures were invisible.** `scrape_finnpanel` swallowed every exception and
returned `[]`; the job only failed when *both* periods produced nothing. One
service changing its markup would have produced a silently-committed 40-row file
on a green run. Git history shows this already cost three days —
2025-08-24/25/26 are missing, repaired by a manual workflow edit on 2025-08-27.

**The dashboard served the wrong day to half the world.** `formatDate` parsed
`YYYY-MM-DD` with `new Date()` (UTC midnight) then read it back with local-time
getters. Verified: selecting 2026-08-21 fetched 2026-08-21 under
`Europe/Helsinki` but **2026-08-20** under `America/New_York`.

**`README.md` was truncated and rewritten daily**, so no documentation could
survive in it.

### Format decision

Five storage strategies were measured by replaying all 719 days of real data into
fresh git repositories, one commit per day, then `git gc --aggressive`:

| Strategy | Git pack | Full clone |
|---|---|---|
| A. `.xlsx` per day (v1) | 8.12 MiB | ~20 MB |
| **B. CSV per day** | **1.45 MiB** | **~7.2 MB** |
| C. One consolidated CSV | 1.00 MiB | ~6.5 MB |
| D. Per-year CSV | 0.97 MiB | ~6.5 MB |
| E. Per-year Parquet, rewritten daily | **22.26 MiB** | ~22.5 MB |

Two results drove the design:

1. **Nearly all the benefit comes from leaving `.xlsx`, not from restructuring.**
   B is a two-line change and captures 64% of a possible 68%. `.xlsx` is a
   pre-compressed ZIP, so git can neither delta nor compress it.
2. **Parquet is a trap.** Smallest file of all five (0.29 MB, 38× smaller than
   CSV) but the *worst repository*, nearly 3× v1, because a compressed binary
   rewritten daily gives git 719 incompressible blobs instead of text deltas.
   **In a commit-every-day repo, how well a format deltas beats how small it is.**

Chose B (per-day CSV under `data/<year>/`) plus a consolidated
`data/finnpanel_all.csv` for convenience.

### Decisions worth recording

**Partial data is committed, *then* the run fails.** The obvious design — refuse
to write anything incomplete — throws away data that can never be recovered.
Writing it and going red preserves the data *and* raises the alarm. Missed days
are permanent; that asymmetry decides it.

**Legacy `.xlsx` were moved, not converted.** Converting would keep both copies
in git history forever to save a few MB of working tree. `git mv` recorded 1,435
renames at almost no cost, and the data is represented in open format inside the
consolidated CSV anyway.

**The scraper no longer writes to GitHub.** v1 committed through the PyGithub
Contents API, one commit per file. v2 writes plain files; the workflow commits
once. Dropped a dependency, removed the PAT requirement, made the script runnable
locally without credentials, and cut three commits a day to one.

**The dashboard reads `raw.githubusercontent.com`, not its own Pages bundle.**
GitHub does not trigger workflows from `GITHUB_TOKEN` commits, so the Pages
deploy never fires from the daily run and the bundled data would be stale.
Reading raw from `main` sidesteps it. *Corollary: edits to `index.html` need a
manual Pages run.*

**The workflow pushes to `$GITHUB_REF_NAME`, not a hardcoded `main`.** Caught
while preparing the test drive: dispatching the workflow on a branch would have
pushed branch commits straight to `main`. Fixed in `78d6a18`, which is what made
an end-to-end production test safe to run at all.

### Verification

Data integrity — the binding constraint, so it was checked rather than assumed:

- All **1,435 original `.xlsx` present and byte-identical** (md5 before/after)
- Git recorded **1,435 renames, zero deletions**
- **25 randomly sampled days reproduce exactly** in the consolidated archive
- **719 days / 86,088 rows** preserved, matching the pre-migration count
- Full pipeline re-run from a **fresh clone** produced identical output

Behaviour:

| Suite | Result |
|---|---|
| Live scrape against Finnpanel | 60 rows, 3 services, both periods |
| Dashboard parsing (real SheetJS) | 5/5 — new CSV and legacy `.xlsx` from 2024/2025/2026 |
| Timezone handling | 9/9 across Helsinki, New York, Auckland |
| Failure paths | 14/14 — all-down, partial-scrape, malformed values |

**Production test drive** — workflow dispatched against `enhancements`, green in
25s ([run 32470848610](https://github.com/jondeman/Finnpanel-Scraper/actions/runs/32470848610)).
It committed [`e1a37e1`](../../commit/e1a37e1), whose entire diff is the
`_Päivitetty:` timestamp in the README's generated block — **the data files it
produced were byte-identical to the local run**. That confirms the scrape is
deterministic across machines and Python versions: the daily diff will show only
real data changes, never serialisation churn.

Note the timestamp means a commit lands every day even when the data has not
moved. That is worth keeping: repository activity resets GitHub's 60-day
scheduled-workflow-disable clock.

### Outcome

Root directory: **1,440 entries → 12**. Future growth: **~9.7 → ~3.6 MB/yr**.
Git pack grew 8.12 → 8.65 MiB, entirely the consolidated CSV; the renames cost
nothing.

### Follow-ups

- [ ] Merge `enhancements` → `main`. Until then the 01:00 UTC cron runs v1, which
      works but keeps discarding episode titles.
- [ ] Run the Pages workflow by hand after merging — it will not fire on its own.
- [ ] Remove the now-unused `GT_TOKEN` repository secret once confirmed unused.
      (`FINNPANEL_TOKEN` and the `GIST*` PATs are unrelated to this repo; `gist`
      scope cannot access repositories.)
- [ ] Review pinned action versions and the pandas `<3` ceiling roughly yearly.
- [ ] Python 3.12 EOL lands around 2028.

### Known limitations

- `Episode`, `PeriodStart` and `PeriodEnd` exist only from 2026-08-21 onward.
  History cannot be repaired.
- `Episode` is empty for Ruutu, which folds the episode into `Program` upstream.
- 2025-08-24/25/26 remain missing and always will.
- If the scraper breaks and commits stop for 60 days, GitHub disables the
  schedule automatically and it must be re-enabled by hand.

---

## 2024-09-02 — collection begins

First `14D`/`90D` split files. Three earlier files
(`finnpanel_data_2024-08-30/31`, `09-01`) predate the split and are carried as
`Period = legacy`.
