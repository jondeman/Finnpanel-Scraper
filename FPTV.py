"""Finnpanel collector: online, Total TV and linear TV top lists, written as CSV under data/.

Runs daily from the workflow, which commits what it writes; nothing here talks to the GitHub API,
so it also runs locally without credentials.

  data/online/<YYYY-MM>.csv   Online top lists (Yle Areena, MTV Katsomo, Ruutu), 14 and 90 days,
                              every demo. Collected since 2024-09-02; until 2026-11-24 they were kept
                              as one .xlsx per period per day, now in archive/xlsx/ and converted here.
  data/totaltv/<YYYY-MM>.csv  Total TV (TV + online) 14-day top lists, every demo, from 2026-11-24.
                              Finnpanel keeps no archive of these, so a missed day is lost for good.
  data/tv/<YYYY>.csv          Linear TV broadcasts from the weekly lists: the top 100, the top 50 with
                              each programme's best airing, and every channel's top 20. Finnpanel keeps
                              the last 10 weeks; a week gains its 7-day figure once it is a week old.
  data/manifest.json          Which files exist and what they cover, for the dashboard.

Every part runs even when another fails. What was collected is written, and the script exits
non-zero if anything failed, so the workflow commits the data and still goes red.

Usage: python FPTV.py [--skip-online] [--skip-totaltv] [--skip-tv]
"""
import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = 'https://www.finnpanel.fi'
ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'data'
REQUEST_PAUSE = 0.4  # seconds between requests to finnpanel.fi

# Finnpanel's target groups (kohderyhmät): page name in the URL -> value written to the Demo column
DEMOS = {
    '3plus': '3+',
    'alle45': 'Alle 45',
    '45plus': '45+',
    'ikar2564': '25-64',
}
DEMO_NAMES = {  # Finnpanel's own name for each demo, as printed in the page header
    '3plus': 'Kaikki 3 vuotta täyttäneet',
    'alle45': 'Alle 45-vuotiaat',
    '45plus': '45 vuotta täyttäneet',
    'ikar2564': '25–64-vuotiaat',
}
DEMO_ORDER = list(DEMOS.values())

# The top lists, per kind: the page name of each list in the URL, and how its header names it
ONLINE_SERVICES = {'yle': 'Yle Areena', 'mtv': 'MTV Katsomo', 'sanoma': 'Ruutu'}
GROUPS = {'yle': 'Yle', 'mtv': 'MTV', 'sanoma': 'Sanoma'}
LIST_KINDS = {
    'online': {
        'folder': 'online', 'names': ONLINE_SERVICES, 'name_column': 'Service', 'rank_column': 'ServiceRank',
        'periods': {'14D': ('online14', 'Viimeiset 14 päivää'), '90D': ('online90', 'Viimeiset 90 päivää')},
    },
    'totaltv': {
        'folder': 'totaltv', 'names': GROUPS, 'name_column': 'Group', 'rank_column': 'GroupRank',
        'periods': {'14D': ('total14', 'TV + online, viimeiset 14 päivää')},
    },
}

# Linear channels that belong to a TotalTV broadcaster group; the rest (TV5, Kutonen, Frii, TLC,
# National Geographic, STAR Channel...) belong to other companies and have no TotalTV list
CHANNEL_GROUPS = {
    'yle tv1': 'Yle', 'yle tv2': 'Yle', 'yle teema&fem': 'Yle', 'yle teema & fem': 'Yle',
    'mtv3': 'MTV', 'mtv sub': 'MTV', 'mtv ava': 'MTV', 'sub': 'MTV', 'ava': 'MTV',
    'nelonen': 'Sanoma', 'jim': 'Sanoma', 'liv': 'Sanoma', 'hero': 'Sanoma',
}
# The weekly top 100 and top 50 call some channels by another name than their own channel list
CHANNEL_ALIASES = {'tv viisi': 'tv5'}
WEEKDAYS = {'ma': 1, 'ti': 2, 'ke': 3, 'to': 4, 'pe': 5, 'la': 6, 'su': 7}
ARCHIVE_WEEKS = 10  # how many weekly lists Finnpanel keeps online

# Rank is across the three services or groups within a demo (as the daily lists have always been
# ranked); ServiceRank and GroupRank are Finnpanel's own rank within one service's or group's list.
# Online rows converted from the xlsx archive have no ServiceRank, PeriodStart, PeriodEnd or Population.
ONLINE_COLUMNS = ['Date', 'Period', 'Demo', 'Rank', 'Service', 'Program', 'Episode', 'Duration', 'Viewers',
                  'ServiceRank', 'PeriodStart', 'PeriodEnd', 'Population']
TOTALTV_COLUMNS = ['Date', 'PeriodStart', 'PeriodEnd', 'Demo', 'Population', 'Rank', 'GroupRank',
                   'Group', 'Program', 'Episode', 'Duration', 'Viewers']
TV_COLUMNS = ['Year', 'Week', 'Date', 'Weekday', 'Time', 'Channel', 'Group', 'Program', 'Reach',
              'ViewersSameDay', 'Viewers7d', 'ChannelRank', 'Top100Rank', 'Top50Rank', 'Fetched']
COLUMNS = {'online': ONLINE_COLUMNS, 'totaltv': TOTALTV_COLUMNS}


class NotAvailable(Exception):
    """Finnpanel answered, but the report does not exist (it serves those as 200 pages)."""


class Fetcher:
    def __init__(self):
        self.session = requests.Session()
        self.last = 0.0

    def get(self, path):
        wait = REQUEST_PAUSE - (time.monotonic() - self.last)
        if wait > 0:
            time.sleep(wait)
        url = BASE_URL + path
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=30)
                self.last = time.monotonic()
                if response.status_code >= 500:
                    raise requests.HTTPError(f'{response.status_code} from {url}')
                response.raise_for_status()
                text = response.content.decode('utf-8-sig')
                if 'ei ole saatavilla' in text:
                    raise NotAvailable(url)
                return text
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as error:
                if attempt == 2:
                    raise
                logging.warning(f'Retrying {url} after: {error}')
                time.sleep(2 * (attempt + 1))


def to_int(value):
    digits = re.sub(r'\D', '', str(value))
    return int(digits) if digits else None


def finnish_date(text):
    day, month, year = (int(part) for part in text.strip().split('.'))
    return date(year, month, day)


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, '') for column in columns})


def data_files(folder):
    return sorted((DATA / folder).glob('[0-9][0-9][0-9][0-9]*.csv'))


def sort_list_rows(rows):
    """Date, then period (online), then demo in Finnpanel's order, then rank."""
    return sorted(rows, key=lambda row: (row['Date'], row.get('Period', ''),
                                         DEMO_ORDER.index(row['Demo']) if row['Demo'] in DEMO_ORDER else 9,
                                         int(row['Rank'] or 0)))


# ---------------------------------------------------------------- daily top lists: online and Total TV

def report_header(soup):
    header = soup.find(id='report_header')
    if not header:
        return []
    return [div.get_text(' ', strip=True) for div in header.find_all('div')]


def parse_top_list(html, kind, period, list_key, demo_key):
    """One top list. Refuses any page that is not the list asked for: Finnpanel answers unknown or
    expired report URLs with another report rather than an error."""
    spec = LIST_KINDS[kind]
    soup = BeautifulSoup(html, 'html.parser')
    header = report_header(soup)
    text = ' | '.join(header)
    name = spec['names'][list_key]
    description = spec['periods'][period][1]
    if not header or header[0] != name:
        raise ValueError(f'Expected the {name} list: {text!r}')
    if description.lower() not in text.lower() or (kind == 'online' and 'TV + online' in text):
        raise ValueError(f'Expected {description!r}: {text!r}')
    if DEMO_NAMES[demo_key] not in text:
        raise ValueError(f'Expected demo {DEMO_NAMES[demo_key]!r}: {text!r}')
    window = re.search(r'Ajanjakso:\s*([\d.]+)\s*[–-]\s*([\d.]+)', text)
    population = re.search(r'Väestö:\s*([\d\s ]+)', text)
    table = soup.find('table', class_='totaltv')
    if not window or not table:
        raise ValueError(f'No period or table on the {name} {demo_key} page')
    rows = []
    for tr in table.find_all('tr'):
        cells = tr.find_all(['th', 'td'])
        if len(cells) != 5 or cells[0].name != 'th':
            continue
        rank = to_int(cells[0].get_text())
        viewers = to_int(cells[4].get_text())
        if rank is None or viewers is None:
            continue
        rows.append({
            'PeriodStart': finnish_date(window.group(1)).isoformat(),
            'PeriodEnd': finnish_date(window.group(2)).isoformat(),
            'Demo': DEMOS[demo_key],
            'Population': to_int(population.group(1)) if population else '',
            spec['rank_column']: rank,
            spec['name_column']: name,
            'Program': cells[1].get_text().strip(),
            'Episode': cells[2].get_text().strip(),
            'Duration': cells[3].get_text().strip(),
            'Viewers': viewers,
        })
    if not rows:
        raise ValueError(f'No rows on the {name} {demo_key} page')
    return rows


def collect_top_lists(fetcher, today, kind):
    """Every list of one kind for today, into that month's file. Returns the number of failed pages.
    A rerun on the same day replaces the day's rows for each period it collected again."""
    spec = LIST_KINDS[kind]
    collected, failures = [], 0
    for period, (page, _) in spec['periods'].items():
        for demo_key in DEMOS:
            demo_rows = []
            for list_key in spec['names']:
                path = f'/tulokset/totaltv/{list_key}/{page}/{demo_key}.html'
                try:
                    rows = parse_top_list(fetcher.get(path), kind, period, list_key, demo_key)
                    demo_rows.extend(rows)
                except Exception as error:
                    failures += 1
                    logging.error(f'{kind} {path} failed: {error}')
            # Rank across the three lists within the demo, as the daily lists have always been ranked
            demo_rows.sort(key=lambda row: (-row['Viewers'], row[spec['rank_column']]))
            for rank, row in enumerate(demo_rows, start=1):
                row.update(Date=today.isoformat(), Period=period, Rank=rank)
            collected.extend(demo_rows)
            logging.info(f'{kind} {period} {DEMOS[demo_key]}: {len(demo_rows)} rows')

    if not collected:
        logging.error(f'{kind}: nothing collected')
        return failures or 1
    path = DATA / spec['folder'] / f'{today:%Y-%m}.csv'
    replaced = {(row['Date'], row['Period']) for row in collected}
    kept = [row for row in read_csv(path) if (row['Date'], row.get('Period', '14D')) not in replaced]
    write_csv(path, COLUMNS[kind], sort_list_rows(kept + collected))
    logging.info(f'{kind}: {len(collected)} rows for {today} written to {path.relative_to(DATA.parent)}')
    return failures


# ---------------------------------------------------------------- linear TV broadcasts, weekly

def week_back(year, week, weeks):
    monday = date.fromisocalendar(year, week, 1) - timedelta(weeks=weeks)
    iso = monday.isocalendar()
    return iso[0], iso[1]


def parse_week_csv(text):
    """A weekly list as CSV: title, (channel), 'viikko W/YYYY', then a ;-separated table.
    Columns differ between lists and between weeks, so they are found by header text."""
    lines = text.splitlines()
    channel = lines[1].strip() if len(lines) > 1 else ''
    header_index = next((i for i, line in enumerate(lines) if line.startswith('#;')), None)
    if header_index is None:
        raise ValueError('No table in the CSV')
    header = lines[header_index].split(';')

    def column(*needles):
        for index, name in enumerate(header):
            if all(needle in name for needle in needles):
                return index
        return None

    columns = {
        'rank': 0, 'program': column('Ohjelma'), 'channel': column('Kanava'), 'day': column('Päivä'),
        'time': column('Kello'), 'reach': column('Tavoittavuus'), 'same_day': column('esitys'),
        'seven_day': column('7 vrk'),
    }
    rows = []
    for line in lines[header_index + 1:]:
        cells = line.split(';')
        if not cells[0].isdigit() or len(cells) < len(header):
            continue

        def cell(key):
            index = columns[key]
            return cells[index].strip() if index is not None else ''

        rows.append({
            'rank': int(cells[0]), 'program': cell('program'), 'channel': cell('channel') or channel,
            'day': cell('day'), 'time': cell('time'), 'reach': to_int(cell('reach')),
            'same_day': to_int(cell('same_day')), 'seven_day': to_int(cell('seven_day')),
        })
    return channel, rows


def collect_week(fetcher, year, week, today):
    """All broadcasts in one week's lists, merged into one row per broadcast."""
    base = '/tulokset/tv/vko'
    index = fetcher.get(f'{base}/top/{year}/{week}/top.html')
    channel_slugs = sorted(set(re.findall(rf'{base}/top/{year}/{week}/([a-z0-9]+)\.html', index)) - {'top'})
    if not channel_slugs:
        raise ValueError(f'No channel lists for week {week}/{year}')

    names = {}  # casefolded channel name -> Finnpanel's display name
    lists = []
    for slug in channel_slugs:
        channel, rows = parse_week_csv(fetcher.get(f'{base}/top/{year}/{week}/{slug}.csv'))
        names[channel.casefold()] = channel
        lists.append(('ChannelRank', rows))
    for kind, rank_column in (('top100', 'Top100Rank'), ('top50p', 'Top50Rank')):
        _, rows = parse_week_csv(fetcher.get(f'{base}/{kind}/{year}/{week}/index.csv'))
        lists.append((rank_column, rows))

    broadcasts = {}
    for rank_column, rows in lists:
        for row in rows:
            folded = row['channel'].casefold()
            channel = names.get(CHANNEL_ALIASES.get(folded, folded), row['channel'])
            key = (channel, row['day'], row['time'], row['program'])
            if key not in broadcasts:
                weekday = WEEKDAYS.get(row['day'])
                broadcasts[key] = {
                    'Year': year, 'Week': week,
                    'Date': date.fromisocalendar(year, week, weekday).isoformat() if weekday else '',
                    'Weekday': row['day'], 'Time': row['time'], 'Channel': channel,
                    'Group': CHANNEL_GROUPS.get(channel.casefold(), ''), 'Program': row['program'],
                    'Reach': row['reach'], 'ViewersSameDay': row['same_day'],
                    'Viewers7d': row['seven_day'] or '', 'Fetched': today.isoformat(),
                }
            broadcast = broadcasts[key]
            broadcast[rank_column] = row['rank']
            if row['seven_day'] and not broadcast['Viewers7d']:
                broadcast['Viewers7d'] = row['seven_day']
    logging.info(f'Week {week}/{year}: {len(broadcasts)} broadcasts from {len(lists)} lists')
    return list(broadcasts.values())


def collect_tv(fetcher, today):
    """Fetches weeks that are missing, and weeks still waiting for their 7-day figures.
    Returns the number of failed weeks."""
    latest_page = fetcher.get('/tulokset/tv/vko/top100/viimeisin/')
    found = re.search(r'viikko\s+(\d+)/(\d{4})', latest_page)
    if not found:
        logging.error('Weekly lists: could not read the latest week')
        return 1
    latest = (int(found.group(2)), int(found.group(1)))

    stored = {}
    for path in data_files('tv'):
        for row in read_csv(path):
            stored.setdefault((int(row['Year']), int(row['Week'])), []).append(row)

    failures = 0
    fetched = {}
    for back in range(ARCHIVE_WEEKS - 1, -1, -1):
        week = week_back(*latest, back)
        rows = stored.get(week)
        waiting_for_7d = rows is not None and week != latest and not any(row['Viewers7d'] for row in rows)
        if rows is not None and not waiting_for_7d:
            continue
        try:
            fetched[week] = collect_week(fetcher, *week, today)
        except NotAvailable as error:
            logging.info(f'Week {week[1]}/{week[0]} not available: {error}')
        except Exception as error:
            failures += 1
            logging.error(f'Week {week[1]}/{week[0]} failed: {error}')

    if fetched:
        stored.update(fetched)
        for year in sorted({week[0] for week in fetched}):
            rows = [row for week, week_rows in stored.items() if week[0] == year for row in week_rows]
            rows.sort(key=lambda row: (int(row['Week']), row['Date'], row['Channel'], row['Time'], row['Program']))
            write_csv(DATA / 'tv' / f'{year}.csv', TV_COLUMNS, rows)
        logging.info(f'Weekly lists: wrote weeks {sorted(fetched)}')
    else:
        logging.info('Weekly lists: nothing new')
    return failures


# ---------------------------------------------------------------- manifest

def write_manifest():
    manifest = {'updated': datetime.now().isoformat(timespec='seconds')}
    for folder in ('online', 'totaltv', 'tv'):
        files = data_files(folder)
        rows = [row for path in files for row in read_csv(path)]
        dates = sorted({row['Date'] for row in rows if row['Date']})
        manifest[folder] = {
            'files': [path.relative_to(ROOT).as_posix() for path in files],
            'first': dates[0] if dates else None,
            'last': dates[-1] if dates else None,
        }
        if folder == 'tv':
            manifest[folder]['weeks'] = sorted({f"{row['Year']}-W{int(row['Week']):02d}" for row in rows})
    (DATA / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--skip-online', action='store_true')
    parser.add_argument('--skip-totaltv', action='store_true')
    parser.add_argument('--skip-tv', action='store_true')
    args = parser.parse_args()

    today = datetime.now().date()
    fetcher = Fetcher()
    failed = []
    steps = [
        ('Online lists', args.skip_online, lambda: collect_top_lists(fetcher, today, 'online')),
        ('Total TV lists', args.skip_totaltv, lambda: collect_top_lists(fetcher, today, 'totaltv')),
        ('Weekly TV lists', args.skip_tv, lambda: collect_tv(fetcher, today)),
    ]
    for name, skip, step in steps:
        if skip:
            continue
        try:
            if step():
                failed.append(name)
        except Exception as error:
            logging.exception(f'{name} failed: {error}')
            failed.append(name)
    write_manifest()
    if failed:
        logging.error(f'Finished with failures in: {", ".join(failed)}')
        sys.exit(1)
    logging.info('Finished')


if __name__ == '__main__':
    main()
