"""One-off conversion of the online-list archive (archive/xlsx/) into data/online/<YYYY-MM>.csv.

Run once on 2026-11-24, when the daily scraper switched from .xlsx to CSV. Kept for provenance:
rerunning it rewrites data/online/ from the archive alone, so do not rerun it after new days have
been collected (it would drop them).

- 14D_/90D_Finnpanel_data_<date>.xlsx (2024-09-02 → 2026-11-24): one file per period per day.
- finnpanel_data_<date>.xlsx (2024-08-30 → 2024-09-01): written before the 14D/90D split by a
  scraper that fetched only the online14 3plus pages (commit 976adc7), so they are 14D, 3+.

Needs openpyxl (pip install openpyxl). Usage: python archive/convert_xlsx.py
"""
import re
import sys
from collections import defaultdict
from pathlib import Path

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from FPTV import DATA, ONLINE_COLUMNS, sort_list_rows, to_int, write_csv  # noqa: E402

ARCHIVE = ROOT / 'archive' / 'xlsx'


def read_xlsx(path):
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        values = list(workbook.worksheets[0].iter_rows(values_only=True))
    finally:
        workbook.close()
    header = [str(name) for name in values[0]]
    rows = []
    for raw in values[1:]:
        record = dict(zip(header, raw))
        viewers = to_int(record.get('Viewers'))
        if viewers is None:
            continue
        rows.append({
            'Demo': record.get('Demo') or '3+',
            'Rank': to_int(record.get('Rank')) or '',
            'Service': record.get('Service') or '',
            'Program': str(record.get('Program') or '').strip(),
            'Episode': str(record.get('Episode') or '').strip(),
            'Duration': str(record.get('Duration') or '').strip(),
            'Viewers': viewers,
        })
    return rows


def main():
    by_month = defaultdict(list)
    files = 0
    for path in sorted(ARCHIVE.glob('*.xlsx')):
        found = re.match(r'(?:(14D|90D)_Finnpanel|finnpanel)_data_(\d{4}-\d{2}-\d{2})\.xlsx$', path.name)
        if not found:
            sys.exit(f'Unexpected file in the archive: {path.name}')
        period, day = found.group(1) or '14D', found.group(2)
        rows = read_xlsx(path)
        if not rows:
            sys.exit(f'No rows in {path.name}')
        by_month[day[:7]].extend({'Date': day, 'Period': period, **row} for row in rows)
        files += 1

    total = 0
    for month, rows in sorted(by_month.items()):
        write_csv(DATA / 'online' / f'{month}.csv', ONLINE_COLUMNS, sort_list_rows(rows))
        total += len(rows)
    print(f'{files} files, {total} rows, {len(by_month)} monthly files written to data/online/')


if __name__ == '__main__':
    main()
