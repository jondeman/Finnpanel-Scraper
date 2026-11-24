# Devlog

Chronological record of substantive changes: what was done, what was measured,
and why each decision went the way it did. Newest first.

---

## 2026-11-24 — Total TV, linear TV, title pages, and everything in CSV

Branch `totaltv-titles`, pushed to `main`. `FPTV.py` replaces `FPGH.py` and
collects every list as CSV under `data/`. The daily `.xlsx` files are converted
and moved to `archive/xlsx/`. The dashboard is split into `assets/`, gains a
Total TV tab and title pages, and reads only CSV.

### Why

The dashboard only showed online viewing. The brief: collect Finnpanel's Total TV
figures (TV and online together) every day for every demo, find out whether the
weekly broadcast lists can be combined with them, and give every programme a
title page with all of its figures and their history, for professional use.

### What Finnpanel publishes

`www.finnpanel.fi` was reachable this time, so everything below was measured
against the live site.

| Source | Content | Demos | Archive |
|---|---|---|---|
| TotalTV `total14` | TV + online, most-watched episode per programme, top 20 per group (Yle, MTV, Sanoma) | 3+, Alle 45, 45+, 25–64 | **None** |
| Online `online14`/`online90` | Already collected by `FPGH.py` | same four | none (ours since 2024-09-02) |
| Weekly `top100`, `top50p`, channel `top` | One row per airing: channel, day, time, reach, average viewers | 3+ only | **10 weeks** |
| Monthly `ohjika` | Top 20 airings per age group | 4+, 4–9, 10–24, 25–44, 45–64, 65+, by sex | back to 2024 at least |

- **Only `total14` exists.** `total7`, `total28` and `total90` answer 200 with
  the online-90 page. Finnpanel serves unknown or expired reports as ordinary
  pages ("Valittua raporttia ei ole saatavilla", or another report), so the
  scraper checks each page's own header before trusting it.
- **The online and TotalTV windows are the same 14 days** (9.–22.11.2026 for
  both on 24.11.), ending two days before the list date.
- **Weekly lists gain a 7-day figure** ("7 vrk:n aikana") once the week is a week
  old; the newest week has same-day viewing only. Columns differ between lists
  and weeks (some have `Kanava`, some the 7-day column), so they are read by
  header text.
- **Weekly lists cut names at 40 characters**, TotalTV and online at 50. The top
  100 calls TV5 "TV Viisi".
- The channel lists cover 16 channels × 20 airings. The top 100 alone is mostly
  Yle TV1 and MTV3 news.

### Can the broadcast lists be combined with Total TV?

Not into one ranked list: they count different things. A weekly row is one
airing's linear audience, measured in the week it aired, for 3+ only. A TotalTV row
is a programme's best episode over a rolling 14 days, on TV and online, in four
demos. Per title they combine well. Measured on the 9.–22.11. window (weeks 46–47):

| Check | Result |
|---|---|
| TotalTV 3+ titles with a linear airing in the same window | **55/60**. The misses are online-led (Pirunpeli, Linna, Bachelor Suomi) or below the channel cut-off |
| TotalTV ÷ best single airing (7-day) | median **1.40**, quartiles 1.03–2.31; news and weather ≈1.0, drama and reality 2–3×, *IS esittää* 8.6× |
| Online share (online-14 ÷ TotalTV, same window) | 34/60 in both lists; median **12%**, range 3–100% (Syke was online-only) |
| Titles the weekly lists add | 300 unique titles in two weeks, **231 not in any TotalTV list**, 89 of them on channels with no TotalTV list (TV5, Kutonen, Frii, TLC, NatGeo, STAR) |
| TotalTV Alle 45 + 45+ vs 3+ | 40 titles in all three: −1k…+54k, median +0.5k. They add up unless the lists name different episodes |

So the dashboard combines the sources per title, not as one ranking. The Total
TV view splits each bar into its online part and the rest, and the title page
plots all three sources on one axis.

### Storage: is GitHub safe, and is this economical?

Measured by replaying the 49 days since 2026-10-07 as daily commits in a fresh
repository, then `git gc --aggressive`:

| | Git growth, 49 days | Per year |
|---|---|---|
| Two `.xlsx` a day (until now) | 1.37 MiB | ≈10 MiB |
| One appended CSV file (now) | 0.43 MiB including the base file | ≈1–3 MiB |

The repository was 11 MiB packed (GitHub reports 11.6 MB). GitHub's guidance is
under 1 GB, so storage was never at risk: even `.xlsx` alone would take about 75
years to get there. But `.xlsx` is the least economical format (zipped, so git
cannot delta it: about 4× CSV), and the root folder had 1,625 files, growing by
730 a year, past the 1,000 that GitHub's file browser lists. Hence the switch
to CSV only.

### Design

**One collector, `FPTV.py`, CSV only.** It replaces `FPGH.py` and its upload
through the GitHub Contents API (one commit per file). It writes plain files,
and the workflow commits them in one commit a day. It runs locally without
credentials and needs only `requests` and `beautifulsoup4` (pandas, numpy,
openpyxl and PyGithub dropped). Online, Total TV and weekly lists are
independent steps. Each writes what it got, and the script exits non-zero after
writing if anything failed. The workflow commits even after a failure, so partial
data is kept and the run still goes red.

- `data/online/<YYYY-MM>.csv`: online top lists, 14 and 90 days, four demos,
  480 rows a day. New rows add `ServiceRank` (Finnpanel's rank within the
  service), `PeriodStart`, `PeriodEnd` and `Population`, and keep the episode.
  `Rank` is still across the three services within a demo.
- `data/totaltv/<YYYY-MM>.csv`: 240 rows a day (3 groups × 4 demos × 20), same
  shape with `Group` and `GroupRank`.
- `data/tv/<YYYY>.csv`: one row per airing, merged from the top 100, the top 50
  and every channel's top 20, with `Viewers7d` filled in when published. Each day
  the script fetches weeks it lacks and weeks still waiting for their 7-day
  figure. A quiet day costs one request.
- `data/manifest.json`: which files exist and what they cover.

**Monthly files** keep a day's list small for the dashboard (the current month
of online lists is 98 KB gzipped, instead of a whole year growing towards
1.2 MB), while a title page reads all of them at once (≈0.8 MB gzipped today).
A rerun on the same day replaces that day's rows.

**The `.xlsx` archive** moved to `archive/xlsx/` with `git mv` (1,625 renames,
contents identical by md5) and was converted by `archive/convert_xlsx.py`. The
three `finnpanel_data_*` files from before the 14D/90D split are 14D: the
scraper of the time fetched only `online14/3plus` (commit `976adc7`).

**Dashboard.** Tabs for Online services and Total TV, and a title search. The
Total TV tab has three views. *TV + online* is a bar per programme, split into
the online part (same programme in the online list for the same 14 days) and the
rest, sortable by total, online and online share. *List* adds online viewers,
share and best linear airing. *Compare demos* is the existing butterfly on
TotalTV data. Every programme name links to its title page (`#/title/<key>`).
A date without a list shows the newest list before it, and says so. The start
date is the newest list (it was yesterday). The download buttons still produce
`.xlsx`, built in the browser.

**Title page.** A headline figure and up to four stat tiles. Then: average
viewers over time (Total TV line, online line with a 14/90-day switch, linear
airings as dots); under 45 vs 45+ over time; the latest demo card per source
(the Compare demos card reused); every linear airing; the most-watched episodes;
and every row with an xlsx download. Demo and range controls scope the whole
page.

### Decisions worth recording

**Titles are matched on the first 40 characters of a normalised name.** Ruutu's
`- Kausi N - Jakso M date` tail, age ratings like `(12)` and case are removed.
40 is where the weekly lists cut names. Linear airings must also be on one of
the group's channels to count toward a TotalTV row.

**Lines break when a programme leaves a list** (gap over two days) rather than
bridging, and list figures are plotted at their window's last day so they line up
with airing dates. Archive rows have no window; theirs is the 14 or 90 days
ending two days before the list date, as the headers showed on 2026-08-21 and
2026-11-24.

**Archive rows have no `ServiceRank` in the CSV.** The dashboard works it out as
the row's position by viewers within its service's list (ties share a rank)
rather than writing a derived value into the data.

**Colours.** The age groups were blue (under 45) and orange (45+) since
2026-10-07, but blue against red or orange reads as men and women. Now yellow
`#eda100` for under 45 and violet `#6250d6` for 45+: no gender reading, light for
the younger group and dark for the older, and far apart (CVD ΔE 37.9,
normal-vision 42.1). The sources take the hues the age groups left: blue
`#2a78d6` (Total TV), aqua `#1baf7a` (online) and orange `#eb6834` (linear),
validated as a set (all-pairs CVD ΔE 9.2, normal-vision 24.0), so no hue means
two things on a title page. A fourth source hue for online 90 days fails the
floors, so 90 days is a switch instead. Yellow and aqua are under 3:1 on white,
so every bar and line carries a visible label and every value is in a table.
The colours are defined once, as CSS variables in `assets/dashboard.css`.

**Online episode titles were never saved.** `FPGH.py` read the episode only if
`len(cols) > 5`, but rows have five cells. `FPTV.py` reads the `Jakso` cell; on
the first run 233 of 480 online rows had an episode.

**Ties.** `FPGH.py` ranked with pandas' unstable sort, so tied programmes came
in arbitrary order. `FPTV.py` breaks ties by the service's own rank. On
2026-11-24 a fresh scrape matched that morning's `.xlsx` on every viewer figure;
95 rows differed only in the order of ties.

### Verification

| Check | Result |
|---|---|
| Archive conversion | 1,625 `.xlsx`, 115,128 rows, 28 monthly files; every file re-read with pandas and compared row by row: 0 mismatches, no CSV day without a source file |
| Fresh online scrape vs that morning's `.xlsx` | Same 480 programmes and viewers; ties ordered by service rank |
| `FPTV.py` run on Python 3.10 with only `requests` + `beautifulsoup4` | Online 480 rows, Total TV 240 rows, weekly lists up to date (weeks 38–47, 2,955 airings, 7-day figure on all but the newest) |
| Second run | Only the manifest timestamp changes |
| `tests/test_fptv.py` | 9/9 on Python 3.13 and 3.10 |
| Browser test (Chromium, local data) | Both tabs, all views, filters, search (1,792 titles), six title pages including a linear-only one (Jeopardy! Suomi), crosshair tooltip, demo and range switches, back link; a missing day falls back to the day before; no `.xlsx` requested; console clean |
| Phone width (390px) | No sideways scroll on Total TV, title page or online list |
| Title page speed | 144 ms to parse and index all history; 16 ms to switch titles |

### Follow-ups

- [ ] Check the first scheduled run (01:00 UTC): it is the first to run `FPTV.py`
      in Actions and commit `data/` from the workflow.
- [ ] Monthly age-group lists (`ohjika`): linear demos (10–24, 25–44, 45–64, 65+,
      by sex) with exact dates and ratings, archived back to 2024. Not collected
      yet; they would add a linear demo panel to title pages.
- [ ] `enhancements` (v2) is superseded: CSV storage, one commit a day, no API
      writes and the episode fix are all in `main` now. Close it.

### Known limitations

- Total TV history starts 2026-11-24; linear airings 2026-09-14 (week 38).
- Linear airings are 3+ only, and only airings in some top list are known.
- Online share compares each list's own most-watched episode; ≈ marks a known
  different episode, but Yle lists name no episodes, so some differences go
  unseen.
- Linear figures in the newest week are same-day; earlier weeks are 7-day.
- Titles that change name between seasons appear as separate titles.
- Anything that read the daily `.xlsx` straight from the repository stops at
  2026-11-24; the old files are under `archive/xlsx/`.

---

## 2026-10-08 — Compare demos: highlight cards removed

Dashboard only. The four highlight cards at the top of Compare demos weren't
needed; the view now opens straight on the sort bar and the butterfly chart.
The same answers are one click away: sorting by Under 45, 45+, Youngest or
Oldest puts each card's show first.

Browser check on the 2026-10-07 14-day file: the chart, row detail cards and
sorting work as before (Youngest: Pikku Kakkonen first), no console errors, no
sideways scroll at 390px.

---

## 2026-10-07 — Compare demos view

Dashboard only, no scraper change.

### Why

The first demo release had a "Compare all demos" option that listed all 240
rows one demo after another. The first 60 rows were the 3+ list, so the view
looked identical to 3+ and compared nothing. The brief for its replacement:
show how each show ranks in each demo and what its audience is made of, very
visually, with a simple UI.

### What the data supports

Measured on the 2026-10-07 files before designing anything:

- **Alle 45 and 45+ add up to 3+.** For the 38 shows in all three 14-day lists
  the two groups sum to the 3+ figure within −1k…+4k (Finnpanel rounds to
  thousands). The under/over-45 split of a show's audience is therefore data,
  not an estimate.
- **That fills gaps.** A show missing from one age list can be worked out from
  the other two (Cooper ja Fry: 145k − 134k ≈ 11k under 45).
- **A missing figure still has a ceiling**: at most the 20th place of that
  service's list (e.g. 11k for Yle, Alle 45, 14 days).
- **Shows must be matched by series name.** Ruutu titles carry the episode and
  the most-watched episode differs by demo. Exact-name matches against 3+:
  Alle 45 43/60 (14D) and 31/60 (90D); by series 44/60 and 38/60.
- **90-day figures are less exact**: different episodes per demo leave the two
  groups up to 42k off the 3+ figure, so those splits are marked ≈.
- **Some shows only make one demo's list**: 26 in the 14-day file.

### Design

**A List / Compare demos switch**, replacing the dropdown option. The
comparison has three parts:

1. **Four highlight cards**: biggest show with under-45s, biggest with 45+,
   youngest audience, oldest audience. Each jumps to its show.
2. **A butterfly chart**, one row per show: under-45 viewers to the left in
   blue, 45+ viewers to the right in orange, from a shared centre line on one
   scale. A row's total width is the show's size; its balance is the audience
   mix. Each bar end carries the viewers and the show's rank in that group; the
   big number on the left is its rank among all viewers. Top 20 by default.
3. **A detail card** per show: total viewers as the one large figure, a large
   under-45/45+ split bar, and one rank card per demo with the best marked.

Sorting is one row of buttons (Total, Under 45, 45+, Youngest, Oldest). A
table-based first draft with a separate size bar, rank grid and split bar was
replaced by the butterfly, which shows size and mix in one mark.

### Decisions worth recording

**Colour only for the two age groups.** Under 45 blue `#2a78d6`, 45+ orange
`#eb6834`, everywhere they appear. Validated, not eyeballed: CVD ΔE 24.7,
normal-vision ΔE 33.6, both ≥3:1 on white. Everything else is grey or the
page's own navy.

**Three rank shades, not five.** The middle grey `#898781` carries small text at
neither 4.5:1 with white (3.59) nor with dark ink (4.17), so rank chips use
1–10, 11–30 and 31–60.

**Worked-out figures are drawn faded and marked ≈**; figures off a list show
their ceiling (≤15k) with no bar, so nothing is drawn that isn't known.

**Every bar is labelled**, so the chart needs no gridlines or axis ticks.
Tooltips only add detail; every value is also on the row or in the card.

**Bars grow in once per render** (staggered, 0.7s); switched off under
`prefers-reduced-motion`. Opening a card inserts it without re-rendering, so
the chart doesn't re-animate on every click.

### Verification

| Check | Result |
|---|---|
| Browser test on the real 2026-10-07 files | Salatut elämät: #1, 96k #4 under 45, 105k #4 45+, 48% under 45. Cooper ja Fry: ≈11k under 45 (faded), 134k #1 45+ |
| Highlight cards | Erikoisjoukot 118k, Cooper ja Fry 134k, Pikku Kakkonen ≈94% under 45, La Promesa ≈100% 45+; with a service filter they show the real rank (Yle: Tuuri #5 in Alle 45) |
| Sorting | Under 45: Erikoisjoukot first. 45+: Cooper ja Fry. Youngest: Pikku Kakkonen, Ryhmä Hau, Tuuri. Oldest: La Promesa, Grantchester |
| Card jump below the top 20 | Pikku Kakkonen (#42) expands the list and opens its card |
| Demo-only shows | Nahkametsä: 19k #26 under 45, ≤15k 45+, no 3+ rank |
| Labels | No bar label overflows its half, desktop or 390px |
| Phone width (390px) | No sideways scroll |
| Pre-demo date | Explains that demo data starts 2026-10-07 |
| List view | Existing test updated for the removed dropdown option; passes |

### Follow-ups

- [ ] Group sizes (population per demo) from the scraper, to compare by share
      of each group reached. Do it with the `enhancements` port.
- [ ] Trend line in the detail card once a few weeks of demo data exist.
- [ ] Carry this view into `enhancements` too; that branch rewrote
      `index.html`.

### Known limitations

- Ranks are across all three services within a demo, as in the data files.
- The view works only from 2026-10-07, the first day with demo data.
- The 25–64 group overlaps both halves of the butterfly, so it appears only in
  the detail card.

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
