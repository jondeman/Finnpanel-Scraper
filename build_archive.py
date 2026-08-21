"""Build data/finnpanel_all.csv -- the whole archive as one tidy CSV.

The per-day files under data/<year>/ remain the authoritative record. This
file is a derived convenience view and can always be regenerated from them.

    python build_archive.py              fast: fold today's CSVs into the archive
    python build_archive.py --rebuild    slow: re-read every per-day file

--rebuild reads the 1,435 legacy .xlsx files too, so it takes a couple of
minutes. The daily path only touches the new CSVs and is near-instant.
"""

import argparse
import glob
import logging
import os
import sys

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

ARCHIVE = 'data/finnpanel_all.csv'
COLUMNS = ['Date', 'Period', 'Rank', 'Service', 'Program', 'Episode',
           'Duration', 'Viewers', 'PeriodStart', 'PeriodEnd']


def period_and_date(path):
    """('14D', '2026-08-21') from any historical or current filename."""
    name = os.path.basename(path)
    date = name.replace('.xlsx', '').replace('.csv', '')[-10:]
    if name.startswith('14D_'):
        return '14D', date
    if name.startswith('90D_'):
        return '90D', date
    return 'legacy', date          # the three pre-split files from Aug/Sep 2024


def read_one(path):
    df = pd.read_excel(path) if path.endswith('.xlsx') else pd.read_csv(path)
    period, date = period_and_date(path)
    df['Period'] = period
    # Legacy .xlsx predate these columns; keep the schema rectangular.
    for col in ('Episode', 'PeriodStart', 'PeriodEnd'):
        if col not in df.columns:
            df[col] = pd.NA
    if 'Date' not in df.columns:
        df['Date'] = date
    # 1 = current CSV, 0 = legacy .xlsx. Sorting on this before dedup means a
    # re-scraped CSV always wins over the older .xlsx for the same day, no
    # matter what order glob returned the files in ('.csv' < '.xlsx').
    df['_priority'] = 0 if path.endswith('.xlsx') else 1
    return df[COLUMNS + ['_priority']]


def normalise(df):
    df = df.copy()
    if '_priority' not in df.columns:
        df['_priority'] = 1
    df['Date'] = df['Date'].astype(str).str.slice(0, 10)
    df['Viewers'] = pd.to_numeric(df['Viewers'], errors='coerce').astype('Int64')
    df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce').astype('Int64')
    for col in ('Episode', 'PeriodStart', 'PeriodEnd', 'Program', 'Service',
                'Duration'):
        df[col] = df[col].astype('string')
    return (df.sort_values(['Date', 'Period', 'Rank', '_priority'],
                           kind='mergesort')
              .drop_duplicates(subset=['Date', 'Period', 'Rank'], keep='last')
              .drop(columns='_priority')
              .reset_index(drop=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rebuild', action='store_true',
                        help='re-read every per-day file, including legacy .xlsx')
    args = parser.parse_args()

    paths = sorted(glob.glob('data/*/*.xlsx') + glob.glob('data/*/*.csv'))
    if not paths:
        logging.error('No per-day files found under data/*/ -- refusing to '
                      'write an empty archive.')
        sys.exit(1)

    if args.rebuild or not os.path.exists(ARCHIVE):
        logging.info('Full rebuild from %d per-day files...', len(paths))
        frames = []
        for i, path in enumerate(paths, 1):
            try:
                frames.append(read_one(path))
            except Exception as exc:
                logging.error('Could not read %s: %s', path, exc)
                sys.exit(1)          # never silently drop a day from the archive
            if i % 200 == 0:
                logging.info('  %d/%d', i, len(paths))
        combined = pd.concat(frames, ignore_index=True)
    else:
        existing = pd.read_csv(ARCHIVE, dtype=str)
        csv_paths = sorted(glob.glob('data/*/*.csv'))
        csv_paths = [p for p in csv_paths if os.path.abspath(p) !=
                     os.path.abspath(ARCHIVE)]
        new = [read_one(p) for p in csv_paths]
        combined = pd.concat([existing] + new, ignore_index=True) if new else existing
        logging.info('Folded %d CSV file(s) into %d existing rows',
                     len(new), len(existing))

    before = len(combined)
    combined = normalise(combined)

    # An archive must never shrink. If it would, something is wrong -- stop.
    if os.path.exists(ARCHIVE):
        previous = len(pd.read_csv(ARCHIVE, dtype=str))
        if len(combined) < previous:
            logging.error('Archive would shrink from %d to %d rows. Aborting.',
                          previous, len(combined))
            sys.exit(1)

    combined.to_csv(ARCHIVE, index=False, lineterminator='\n')
    logging.info('Wrote %s: %d rows (%d before dedup), %d days, %.1f MB',
                 ARCHIVE, len(combined), before, combined['Date'].nunique(),
                 os.path.getsize(ARCHIVE) / 1048576)


if __name__ == '__main__':
    main()
