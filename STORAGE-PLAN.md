# Repository Storage Plan

A costed set of options for keeping this scraper running for decades. Every
number below was measured against the real archive on 2026-08-21, not estimated.

> **Status: Tier 1 and Tier 2 are implemented** as of 2026-08-21 — CSV output,
> per-year folders, `.gitattributes`, the consolidated archive, the recovered
> `Episode` / `PeriodStart` / `PeriodEnd` columns, and validation-before-commit.
> Tier 3 remains unimplemented and is not needed for roughly a century. See
> [DOCUMENTATION.md](DOCUMENTATION.md#what-changed-in-v2).

---

## Bottom line

**There is no storage crisis, and there will not be one in your lifetime.** At
the current growth rate the repository reaches GitHub's 1 GB guidance in roughly
**104 years**. Converting to CSV pushes that to ~280 years. Neither number is a
reason to act.

**Convert to CSV anyway** — but for the reasons that actually matter over a
multi-decade run:

1. **Format durability.** `.xlsx` is a versioned binary ZIP that only survives as
   long as `openpyxl` keeps reading it. CSV is readable by anything, forever.
   This is the real long-horizon risk, and it is not a size risk.
2. **Git stops choking.** `.xlsx` is pre-compressed, so git cannot delta or
   compress it. **Measured: the identical data as CSV shrinks git history from
   8.12 MiB to 1.45 MiB — an 82% reduction — with no layout change at all.**
3. **Diffs become readable.** Today every commit is an opaque binary blob. As
   CSV you can see what changed, which is how you would catch the kind of silent
   data corruption described in Finding 5 of [DOCUMENTATION.md](DOCUMENTATION.md).

**The genuinely urgent problem is the file count, not the byte count** — you are
already over GitHub's 1,000-entry directory cap. That is a folder-layout fix, and
it is independent of file format.

---

## Measured baseline (2026-08-21)

| Metric | Value |
|---|---|
| Root tree entries | 1,440 (1,435 `.xlsx` + 5) |
| Working tree, `.xlsx` only | 10.98 MB |
| Git pack (`.git`) | 8.12 MiB |
| **Full clone** | **~20 MB** |
| Commits | 2,551 |
| Rows archived | 86,088 across 719 days |
| Growth | 2 files/day ≈ 730 files and ~9.7 MB of clone per year |

The dataset is tiny: **60 rows a day.** Two years of collection amounts to
5.7 MB as plain CSV. Everything in this document is about tidiness, durability
and usability — the bytes are a rounding error.

---

## What each option actually costs

Strategies B and C were measured by replaying all 719 days of real data into
fresh git repositories, one commit per day, then `git gc --aggressive`.

| Strategy | Git pack | Working tree | Full clone | vs today |
|---|---|---|---|---|
| **A. `.xlsx` per day** — today | 8.12 MiB | 10.98 MB | ~20 MB | — |
| **B. CSV per day**, same flat layout | **1.45 MiB** | 5.67 MB | **~7.2 MB** | **−64%** |
| **C. One consolidated CSV**, appended daily | **1.00 MiB** | 5.51 MB | **~6.5 MB** | **−68%** |
| **D. Per-year CSV**, appended daily | **0.97 MiB** | 5.51 MB | **~6.5 MB** | **−68%** |
| E. Per-year Parquet, rewritten daily | 22.26 MiB | 0.29 MB | ~22.5 MB | **+13%** ⚠️ |

Projected forward:

| Strategy | Clone/yr | 2036 | 2046 | Hits 1 GB |
|---|---|---|---|---|
| A. `.xlsx` per day | 9.7 MB | 110 MB | 207 MB | year **2130** |
| B. CSV per day | 3.6 MB | 41 MB | 77 MB | year **2307** |
| C. Consolidated CSV | 3.3 MB | 38 MB | 71 MB | year **2334** |
| D. Per-year CSV | 3.3 MB | 38 MB | 70 MB | year **2340** |
| E. Per-year Parquet | 11.4 MB | 130 MB | 244 MB | year **2115** |

Note how little separates B, C and D — under 0.5 MiB of history across the whole
two years. **Nearly all the benefit comes from leaving the `.xlsx` format**, not
from restructuring the layout. That matters, because B is a two-line change while
C and D are rewrites of both the scraper and the dashboard.

**Strategy E is the cautionary tale, and it is counter-intuitive.** Parquet
produces by far the smallest *file* — 0.29 MB for the entire archive, 38× smaller
than CSV — yet the **worst repository of all five, nearly 3× today's**. Because
it is compressed binary, rewriting it daily makes git store a fresh,
incompressible ~30 KB blob every single day instead of a small text delta — 719
of them, none of which git can pack against its neighbours. The lesson
generalises: in a
commit-every-day repository, **how well a format deltas matters far more than how
small it is.** Optimise the diff, not the file.

For reference, the whole archive in other formats:

| Format | Size | Note |
|---|---|---|
| 1,435 `.xlsx` | 10.98 MB | today |
| 1,435 `.csv` | 5.67 MB | |
| one `all.csv` | 5.51 MB | |
| one `all.csv.gz` | 0.45 MB | not diffable, defeats git |
| one `all.parquet` (zstd) | **0.29 MB** | 38× smaller — but binary, and see below |

### Relevant GitHub limits

| Limit | Value | Where you stand |
|---|---|---|
| Directory listing in web UI | 1,000 entries | ⚠️ **exceeded** — 440 files hidden |
| Single file, warning | 50 MB | fine — largest is 8 KB |
| Single file, hard block | 100 MB | fine |
| Repository, recommended | 1 GB | ~2% used |
| Repository, strong ceiling | 5 GB | fine |
| GitHub Pages published site | 1 GB | ~2% used |
| Actions minutes, public repo | unlimited | fine |
| **Scheduled workflows** | **disabled after 60 days of repo inactivity** | ⚠️ see below |

That last row is the only limit with real teeth. Daily commits keep the cron
alive — but if the scraper breaks, commits stop, and 60 days later GitHub
switches the schedule off. **A fixable outage silently becomes a permanent one.**
That is a far bigger threat to "running for years" than any byte count.

---

## Recommendation

Three tiers. Each is independently useful; do as many as you have appetite for.

### Tier 1 — Do this (about an hour, low risk)

**1a. Emit CSV instead of XLSX, going forward only.**

In `FPGH.py`, replace the `df.to_excel(...)` / `BytesIO` block with
`df.to_csv(index=False).encode('utf-8')` and change the filename suffix. Leave
the 1,435 existing `.xlsx` files exactly where they are — a mixed archive is
fine, and rewriting history is not worth it.

`index.html` then needs a small branch: parse CSV for new dates, keep the
existing SheetJS path for older ones. SheetJS already reads CSV, so this is a
few lines.

*Effect: −64% on all future growth, readable diffs, format risk gone.*

**1b. Move data into year folders.**

```
data/2024/14D_Finnpanel_data_2024-09-02.csv
data/2025/...
data/2026/...
```

`git mv` the existing files too — git stores the move as a rename, so this costs
almost nothing in pack size. Fixes the 1,000-entry truncation permanently and
makes the repo browsable again. Requires updating `baseUrl` in `index.html`.

*Effect: the one problem you have actually hit, solved.*

**1c. Add a `.gitattributes`.**

```
*.xlsx binary
*.csv  text eol=lf
```

Stops git attempting diffs on the legacy binaries and keeps line endings stable
across contributors.

### Tier 2 — Worth it if you want the archive to be *usable* (half a day)

**2a. Also publish a single consolidated `data/finnpanel_all.csv`,** rebuilt on
each run.

This is the file anyone doing analysis actually wants — one `pd.read_csv` instead
of globbing 1,400 files. At 5.5 MB today and ~2.8 MB/yr it stays comfortable for
decades, and because it is append-only, git deltas it efficiently (measured: 1.0
MiB of history for the full two years).

Keep the per-day files as well. They are the durable record; the consolidated
file is a convenience view that can always be regenerated.

**2b. Add the columns you are currently throwing away.**

Findings 2 and 3 in [DOCUMENTATION.md](DOCUMENTATION.md): `Episode` has been
empty for 86,088 consecutive rows, and the actual measurement window
(`Ajanjakso: 6.8.2026 – 19.8.2026`) is discarded in favour of the scrape date.
Adding `Episode`, `PeriodStart` and `PeriodEnd` roughly doubles row width — from
5.5 MB to perhaps 9 MB for the full archive.

**Spend the bytes.** Storage is free here; the data is not recoverable later.

**2c. Validate before committing.**

Assert 60 rows and 3 distinct services. Fail the run rather than committing a
partial file. Combined with a notification you will actually see, this is the
single highest-value change in this document — missed days cannot be backfilled,
so a loud failure is worth far more than a quiet one.

### Tier 3 — Only if the repo ever genuinely gets large (years away)

**3a. Yearly Parquet snapshots** — but **not committed daily**, per the strategy
E measurement above. If you want Parquet for analytics, either regenerate it in
CI and attach it to a Release, or commit it **once a year** when the year closes
and never touch it again. A frozen yearly Parquet costs ~100 KB forever; a daily
rewritten one costs 22 MiB per two years.

**3b. Publish releases instead of committing.** Attach yearly bundles to GitHub
Releases; release assets do not count toward repository size. Only worth it if
you cross ~1 GB, i.e. around the year 2130.

**3c. Shallow-clone the workflow.** Add `fetch-depth: 1` to `actions/checkout`.
Saves a few seconds per run today; matters more as history grows. Note this is
compatible with the Contents-API upload path, but **not** with the README `git
pull --rebase` step as currently written.

---

## Migration sketch for Tier 1

```bash
# 1. reorganise, preserving history as renames
mkdir -p data/{2024,2025,2026}
for f in *_Finnpanel_data_*.xlsx; do
  y=$(echo "$f" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | cut -d- -f1)
  git mv "$f" "data/$y/$f"
done

# 2. confirm git recorded renames, not delete+add
git status --short | head
git commit -m "Move data files into per-year folders"

# 3. verify pack size did not balloon
git count-objects -vH
```

Then in `FPGH.py`, the filename becomes
`f'data/{current_date[:4]}/{prefix}_Finnpanel_data_{current_date}.csv'` — the
Contents API creates intermediate folders automatically — and in `index.html`,
`baseUrl` gains the `data/${year}/` segment.

**Test on a branch first.** The dashboard reads from `main` via
`raw.githubusercontent.com`, so a bad path breaks it for everyone immediately.

---

## What not to do

- **Do not rewrite history** (`filter-repo`, squashing, orphan branches) to
  reclaim the 8 MiB pack. It breaks every existing clone and permalink to save an
  amount of space that does not matter.
- **Do not delete old files.** They are the entire point of the project, they
  cannot be re-scraped, and they cost ~9.7 MB/yr.
- **Do not commit `.gz`, `.zip` or daily-rewritten Parquet** as the primary
  store. Compressed blobs defeat git's delta compression: measured, daily Parquet
  produces a **22.3 MiB pack against today's 8.12 MiB** — nearly 3× worse — even
  though the file itself is 38× smaller. This is the trap that looks like the
  obvious optimisation.
- **Do not convert the 1,435 existing `.xlsx` files** in place. It doubles the
  pack (both versions live in history forever) to save 5 MB of working tree.
  Switch format going forward and leave the past alone.
- **Do not adopt Git LFS.** It adds quota, cost and a failure mode, for files
  averaging 8 KB.

---

## Summary

| | |
|---|---|
| Is there a size limit you will hit? | **No.** ~104 years away, and the fixes push it past 2300. |
| Should you convert to CSV? | **Yes** — for durability, git efficiency (−82% history) and diffability. Not for space. |
| What is actually urgent? | The **1,000-file directory cap** (already hit) and the **absence of failure alerting** (already cost you three days of data). |
| Biggest risk to running for years? | Not storage — it is a silent breakage that stops commits, after which GitHub disables the cron at day 60. |
