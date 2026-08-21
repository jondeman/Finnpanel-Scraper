"""Finnpanel scraper.

Collects the most-watched programmes on Yle Areena, MTV Katsomo and Ruutu from
finnpanel.fi and stores one CSV per period per day in this repository.

Run modes
---------
    python FPGH.py                    scrape and write CSVs into data/<year>/
    python FPGH.py --dry-run          scrape and print, write nothing

This script only writes files. Committing and pushing them is the workflow's
job (.github/workflows/Finnpanel_Scraper.yml), which keeps this runnable
locally with no token and no network writes.

Output
------
    data/<year>/14D_Finnpanel_data_<date>.csv
    data/<year>/90D_Finnpanel_data_<date>.csv

Files written before 2026-08 are .xlsx in the repository root and lack the
Episode/PeriodStart/PeriodEnd columns; see DOCUMENTATION.md.
"""

import argparse
import logging
import os
import re
import sys
import traceback
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Column order. The first seven match the historical .xlsx layout exactly so
# that old and new files concatenate without fixups; new columns are appended.
COLUMNS = ['Date', 'Rank', 'Service', 'Program', 'Episode', 'Duration',
           'Viewers', 'PeriodStart', 'PeriodEnd']

SERVICE_BY_URL_KEY = {'mtv': 'MTV Katsomo',
                      'sanoma': 'Ruutu',
                      'yle': 'Yle Areena'}

URLS = {
    '14D': ['https://www.finnpanel.fi/tulokset/totaltv/mtv/online14/3plus.html',
            'https://www.finnpanel.fi/tulokset/totaltv/sanoma/online14/3plus.html',
            'https://www.finnpanel.fi/tulokset/totaltv/yle/online14/3plus.html'],
    '90D': ['https://www.finnpanel.fi/tulokset/totaltv/yle/online90/3plus.html',
            'https://www.finnpanel.fi/tulokset/totaltv/mtv/online90/3plus.html',
            'https://www.finnpanel.fi/tulokset/totaltv/sanoma/online90/3plus.html'],
}

EXPECTED_ROWS_PER_SERVICE = 20
USER_AGENT = ('Finnpanel-Scraper/2.0 '
              '(+https://github.com/jondeman/Finnpanel-Scraper)')


def make_session():
    """Session that retries transient failures instead of losing the day."""
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=['GET'])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    session.headers.update({'User-Agent': USER_AGENT})
    return session


def clean_int(value):
    """'1.'->1, '191 000'->191000. Thousands separators only, never decimals."""
    cleaned = value.replace('#', '').replace('\xa0', '').replace(' ', '')
    cleaned = cleaned.replace(' ', '').strip()
    # A trailing '.' is the Finnish ordinal marker on ranks ("1."); interior
    # dots are thousands separators. Neither is ever a decimal point here.
    cleaned = cleaned.rstrip('.').replace('.', '')
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_period(title_text):
    """Pull the real measurement window out of the table's title block.

    'Ajanjakso: 6.8.2026 – 19.8.2026' -> ('2026-08-06', '2026-08-19')

    This is the window the figures actually describe. It is not the same as the
    scrape date -- it typically ends a day or two earlier.
    """
    match = re.search(
        r'Ajanjakso:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s*[–\-—]\s*'
        r'(\d{1,2})\.(\d{1,2})\.(\d{4})', title_text)
    if not match:
        return None, None
    d1, m1, y1, d2, m2, y2 = match.groups()
    return f'{y1}-{int(m1):02d}-{int(d1):02d}', f'{y2}-{int(m2):02d}-{int(d2):02d}'


def scrape_finnpanel(url, session):
    """Scrape one service/period page. Raises on failure -- never returns [] silently."""
    service = next((name for key, name in SERVICE_BY_URL_KEY.items() if key in url),
                   'Unknown')
    logging.info('Scraping %s (%s)', service, url)

    response = session.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table', class_='totaltv')
    if not table:
        raise RuntimeError(f'No <table class="totaltv"> found for {service} at {url}')

    rows = table.find_all('tr')
    period_start, period_end = parse_period(rows[0].get_text(' ', strip=True))
    if not period_start:
        logging.warning('Could not parse Ajanjakso for %s', service)

    data = []
    for row in rows:
        cols = row.find_all(['th', 'td'])
        if len(cols) < 5:
            continue                      # title block
        rank = clean_int(cols[0].get_text())
        viewers = clean_int(cols[-1].get_text())
        if rank is None or viewers is None:
            continue                      # header row ('#', 'Ohjelma', ...)
        data.append({
            'Rank': rank,
            'Service': service,
            'Program': cols[1].get_text().strip(),
            # Historically dropped by an off-by-one guard: rows have exactly 5
            # cells, and the old code required more than 5.
            'Episode': cols[2].get_text().strip(),
            'Duration': cols[-2].get_text().strip(),
            'Viewers': viewers,
            'PeriodStart': period_start,
            'PeriodEnd': period_end,
        })

    if not data:
        raise RuntimeError(f'Table found for {service} but no data rows parsed')

    logging.info('  %s: %d rows, period %s..%s',
                 service, len(data), period_start, period_end)
    return data


def process_data(urls, period, session, scrape_date):
    """Scrape every service for one period. Returns (DataFrame|None, [problems])."""
    all_data, problems = [], []

    for url in urls:
        try:
            all_data.extend(scrape_finnpanel(url, session))
        except Exception as exc:
            problems.append(f'{period}: {url} failed: {exc}')
            logging.error('FAILED %s: %s', url, exc)

    if not all_data:
        problems.append(f'{period}: no data at all from any service')
        return None, problems

    df = pd.DataFrame(all_data)

    # Stable sort so equal viewer counts keep a deterministic order between
    # runs instead of churning the diff.
    df = (df.sort_values(['Viewers', 'Service', 'Program'],
                         ascending=[False, True, True], kind='mergesort')
            .reset_index(drop=True))
    df['Rank'] = df.index + 1
    df['Date'] = scrape_date
    df = df[COLUMNS]

    # Validate. Anything short of a full scrape still gets written -- a partial
    # day beats a missing day, because missed days cannot be backfilled -- but
    # it is recorded as a problem so the run goes red and someone looks.
    found = set(df['Service'])
    for name in SERVICE_BY_URL_KEY.values():
        if name not in found:
            problems.append(f'{period}: {name} is missing entirely')
    for name, count in df['Service'].value_counts().items():
        if count != EXPECTED_ROWS_PER_SERVICE:
            problems.append(
                f'{period}: {name} returned {count} rows, expected '
                f'{EXPECTED_ROWS_PER_SERVICE}')
    if df['PeriodEnd'].isna().any():
        problems.append(f'{period}: measurement window missing for some rows')

    logging.info('%s: %d rows total', period, len(df))
    return df, problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true',
                        help='scrape and print, write nothing')
    parser.add_argument('--out', default='.',
                        help='repository root to write data/ into (default: .)')
    args = parser.parse_args()

    scrape_date = datetime.utcnow().strftime('%Y-%m-%d')
    logging.info('Finnpanel scraper starting, scrape date %s (UTC)', scrape_date)

    session = make_session()
    all_problems, wrote_any = [], False

    for period, urls in URLS.items():
        df, problems = process_data(urls, period, session, scrape_date)
        all_problems.extend(problems)
        if df is None:
            continue

        path = f'data/{scrape_date[:4]}/{period}_Finnpanel_data_{scrape_date}.csv'
        payload = df.to_csv(index=False, lineterminator='\n').encode('utf-8')

        if args.dry_run:
            logging.info('[dry-run] would write %s (%d bytes)', path, len(payload))
            print(df.head(10).to_string(index=False))
        else:
            full = os.path.join(args.out, path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, 'wb') as handle:
                handle.write(payload)
            logging.info('Wrote %s (%d bytes)', path, len(payload))
        wrote_any = True

    if all_problems:
        logging.error('Completed with %d problem(s):', len(all_problems))
        for problem in all_problems:
            logging.error('  - %s', problem)

    if not wrote_any:
        logging.error('Nothing was scraped for either period.')
        sys.exit(1)
    if all_problems:
        # Data that was scraped has been saved; exit non-zero so the run goes
        # red and the workflow opens an issue.
        sys.exit(1)

    logging.info('Done, no problems.')


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        logging.error('Unhandled error:\n%s', traceback.format_exc())
        sys.exit(1)
