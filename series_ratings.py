"""Finnpanel viewing figures for one series, ready to paste into Quickbase.

Reads the CSV files this repository collects (data/totaltv, data/online, data/tv) and prints, for
the series asked for:

  * the newest Total TV figure (TV + online, most-watched episode of the last 14 days) in every demo,
    with the rank on the channel group's top 20,
  * the online (14-day) and linear (weekly) figures for the same window,
  * the best "mention" (e.g. "#12 on MTV's Total TV top 20"),
  * a week-by-week history since --since,
  * a one-line English note for Quickbase (TX Notes). It names only the lists the series is on;
    a series on no list gets no note.

Standard library only, so it runs anywhere with Python 3.8+. Data comes from this checkout when the
script sits next to data/manifest.json, otherwise from GitHub (the repository is public).

  python series_ratings.py "Koko Suomi leipoo" --since 2026-09-01
  python series_ratings.py "Bachelor Suomi" --since 2026-11-16 --json
  python series_ratings.py --search leipoo
"""
import argparse
import csv
import io
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

RAW_BASE = 'https://raw.githubusercontent.com/jondeman/Finnpanel-Scraper/main/'
DASHBOARD = 'https://jondeman.github.io/Finnpanel-Scraper/'

# The dashboard and the note call Finnpanel's "Sanoma" group Nelonen
GROUP_NAMES = {'Yle': 'Yle', 'MTV': 'MTV', 'Sanoma': 'Nelonen'}
GROUP_OF_SERVICE = {'Yle Areena': 'Yle', 'MTV Katsomo': 'MTV', 'Ruutu': 'Sanoma'}
DEMOS = ['3+', 'Alle 45', '45+', '25-64']
DEMO_LABELS = {'3+': '3+', 'Alle 45': 'under 45', '45+': '45+', '25-64': '25-64'}
MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


# ---------------------------------------------------------------- names (same rules as assets/data.js)

def series_name(program):
    """The programme name without what changes between episodes (Ruutu's season/episode tail, a
    publication date, an age rating)."""
    name = str(program or '')
    name = re.sub(r' - Kausi\b.*$', '', name)
    name = re.sub(r' - (K(a(u(s)?)?)?)?$', '', name)
    name = re.sub(r'\s+\d{1,2}\.\d{1,2}\.\d{2,4}( \d{1,2}:\d{0,2})?$', '', name)
    name = re.sub(r'\s*\((S|\d{1,2})\)$', '', name)
    return name.strip()


def title_key(program):
    """One key per programme across all lists: weekly lists cut names at 40 characters."""
    return re.sub(r'\s+', ' ', series_name(program).lower())[:40].strip()


def is_promo(program):
    return str(program or '').lower().startswith('tulossa:')


def matcher(names, excludes=()):
    """True for rows of the series: the same title key as one of `names`, or that key followed by a
    subtitle after a colon ("Pimeä Puoli: Riku Rantala & ..."). Trailers ("Tulossa: ...") never match."""
    keys = [title_key(name) for name in names]
    excluded = {title_key(name) for name in excludes}

    def match(program):
        if is_promo(program):
            return False
        key = title_key(program)
        if key in excluded:
            return False
        return any(key == k or key.startswith(k + ':') for k in keys)
    return match


# ---------------------------------------------------------------- loading

def to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_date(value):
    return date.fromisoformat(value) if value else None


class Source:
    """Reads data/ from a local checkout or from a base URL."""

    def __init__(self, location=None):
        if location is None:
            here = Path(__file__).resolve().parent
            location = str(here) if (here / 'data' / 'manifest.json').exists() else RAW_BASE
        self.location = location
        self.remote = location.startswith('http')

    def read(self, path):
        if self.remote:
            url = self.location.rstrip('/') + '/' + path
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read().decode('utf-8')
        return (Path(self.location) / path).read_text(encoding='utf-8')

    def rows(self, path):
        return list(csv.DictReader(io.StringIO(self.read(path))))


def load(source, since):
    """All rows from the files that can hold data on or after `since`."""
    manifest = json.loads(source.read('data/manifest.json'))
    data = {}
    for kind in ('totaltv', 'online', 'tv'):
        rows = []
        for path in manifest.get(kind, {}).get('files', []):
            stem = Path(path).stem  # 2026-11 or 2026
            if since and stem < since.isoformat()[:len(stem)]:
                continue
            rows.extend(source.rows(path))
        data[kind] = rows
    return data


# ---------------------------------------------------------------- formatting

def num(value):
    return f'{value:,}' if value is not None else '-'


def day_text(d, year=False):
    return f'{d.day} {MONTHS[d.month - 1]}' + (f' {d.year}' if year else '')


def window_text(start, end):
    if start.month == end.month and start.year == end.year:
        return f'{start.day}-{end.day} {MONTHS[end.month - 1]} {end.year}'
    return f'{day_text(start)} - {day_text(end, year=True)}'


def window_of(row):
    """Online archive rows have no window: theirs is the 14 or 90 days ending two days before the list
    date (as Finnpanel's headers show)."""
    start, end = parse_date(row.get('PeriodStart')), parse_date(row.get('PeriodEnd'))
    if start and end:
        return start, end
    listed = parse_date(row['Date'])
    days = 90 if row.get('Period') == '90D' else 14
    end = listed - timedelta(days=2)
    return end - timedelta(days=days - 1), end


def possessive(name):
    return f"{name}'" if name.endswith('s') else f"{name}'s"


# ---------------------------------------------------------------- analysis

def search(data, term):
    """Distinct series names containing `term`, with where and when they were last listed."""
    term = term.lower()
    found = {}
    for kind, column, where in (('totaltv', 'Group', 'Total TV'), ('online', 'Service', 'Online'),
                                ('tv', 'Channel', 'Linear')):
        for row in data[kind]:
            if term not in row['Program'].lower():
                continue
            name = row['Program'] if is_promo(row['Program']) else series_name(row['Program'])
            entry = found.setdefault(name, {'name': name, 'promo': is_promo(row['Program']), 'where': set(),
                                            'last': '', 'rows': 0})
            entry['where'].add(f'{where}: {row[column]}')
            entry['last'] = max(entry['last'], row['Date'])
            entry['rows'] += 1
    return sorted(found.values(), key=lambda e: (e['promo'], e['last']), reverse=False)


def online_service_rank(rows, row):
    """Finnpanel's rank within the service; archive rows lack it, so it is the position by viewers."""
    if to_int(row.get('ServiceRank')):
        return to_int(row['ServiceRank'])
    viewers = to_int(row['Viewers'])
    return 1 + sum(1 for other in rows
                   if other['Date'] == row['Date'] and other['Period'] == row['Period']
                   and other['Demo'] == row['Demo'] and other['Service'] == row['Service']
                   and to_int(other['Viewers']) > viewers)


def best_airing(airings):
    return max(airings, key=lambda a: (to_int(a['Viewers7d']) or to_int(a['ViewersSameDay']) or 0))


def airing_text(airing):
    d = parse_date(airing['Date'])
    seven = to_int(airing['Viewers7d'])
    figure = f'{num(seven)} (7 days)' if seven else f"{num(to_int(airing['ViewersSameDay']))} (same day)"
    return f"{airing['Channel']} {WEEKDAYS[d.weekday()]} {day_text(d)} {airing['Time']}: {figure}"


def analyse(data, names, excludes=(), since=None, as_of=None, group=None):
    match = matcher(names, excludes)
    since = since or date(2000, 1, 1)
    as_of = as_of or date.today()

    def in_range(row):
        d = parse_date(row['Date'])
        return since <= d <= as_of

    totaltv_all = [r for r in data['totaltv'] if in_range(r)]
    online_all = [r for r in data['online'] if in_range(r)]
    tv_rows = [r for r in data['tv'] if in_range(r) and match(r['Program'])]
    totaltv_rows = [r for r in totaltv_all if match(r['Program'])]
    online_rows = [r for r in online_all if match(r['Program'])]

    # Which channel group the series belongs to
    groups = [r['Group'] for r in totaltv_rows] + [GROUP_OF_SERVICE.get(r['Service']) for r in online_rows] \
        + [r['Group'] for r in tv_rows if r['Group']]
    group = group or (max(set(g for g in groups if g), key=groups.count) if any(groups) else None)

    result = {
        'names': names,
        'matched': {
            'totaltv': sorted({r['Program'] for r in totaltv_rows}),
            'online': sorted({series_name(r['Program']) for r in online_rows}),
            'tv': sorted({r['Program'] for r in tv_rows}),
        },
        'group': group,
        'group_name': GROUP_NAMES.get(group, group),
        'dashboard': f"{DASHBOARD}#/title/{urllib.parse.quote(title_key(names[0]))}",
    }

    # ---- snapshot: the newest Total TV list (or online list if Total TV has none yet)
    list_dates = sorted({r['Date'] for r in totaltv_all}) or sorted({r['Date'] for r in online_all})
    snapshot = {}
    if list_dates:
        listed = list_dates[-1]
        sample = next(r for r in (totaltv_all or online_all) if r['Date'] == listed)
        start, end = window_of(sample)
        snapshot = {'list_date': listed, 'start': start.isoformat(), 'end': end.isoformat(),
                    'window': window_text(start, end), 'totaltv': [], 'online': [], 'linear': []}
        for demo in DEMOS:
            for row in totaltv_rows:
                if row['Date'] == listed and row['Demo'] == demo:
                    snapshot['totaltv'].append({
                        'demo': demo, 'viewers': to_int(row['Viewers']), 'episode': row['Episode'],
                        'group': row['Group'], 'group_rank': to_int(row['GroupRank']), 'rank': to_int(row['Rank']),
                        'population': to_int(row['Population'])})
        snapshot['online90'] = []
        for row in online_rows:
            if row['Date'] == listed:
                snapshot['online' if row['Period'] == '14D' else 'online90'].append({
                    'demo': row['Demo'], 'service': row['Service'], 'viewers': to_int(row['Viewers']),
                    'episode': row['Episode'], 'service_rank': online_service_rank(online_all, row)})
        snapshot['linear'] = [r for r in tv_rows if start.isoformat() <= r['Date'] <= end.isoformat()]
    result['snapshot'] = snapshot

    # ---- mentions, best first
    mentions = []
    window = snapshot.get('window')
    tt = snapshot.get('totaltv', [])
    if tt:
        best = min(tt, key=lambda e: (e['group_rank'], DEMOS.index(e['demo'])))
        main = next((e for e in tt if e['demo'] == '3+'), best)
        mentions.append(f"#{main['group_rank']} on {possessive(GROUP_NAMES[main['group']])} Total TV top 20 "
                        f"({DEMO_LABELS[main['demo']]}, {window}), {num(main['viewers'])} viewers")
        if best is not main and best['group_rank'] < main['group_rank']:
            mentions.append(f"#{best['group_rank']} on {possessive(GROUP_NAMES[best['group']])} Total TV top 20 "
                            f"among {DEMO_LABELS[best['demo']]} ({num(best['viewers'])})")
    linear = snapshot.get('linear', [])
    for column, label in (('Top100Rank', "Finnpanel's weekly top 100 airings"),
                          ('Top50Rank', "Finnpanel's weekly top 50 programmes")):
        ranked = [a for a in linear if to_int(a[column])]
        if ranked:
            a = min(ranked, key=lambda a: to_int(a[column]))
            mentions.append(f"#{a[column]} on {label} (week {a['Week']})")
    # The channel rank of the most-watched airing: "#9 on MTV Ava" says less than "#15 on MTV3"
    ranked = [a for a in linear if to_int(a['ChannelRank'])]
    if ranked:
        a = best_airing(ranked)
        mentions.append(f"#{a['ChannelRank']} on {possessive(a['Channel'])} weekly top 20 (week {a['Week']})")
    online_main = [e for e in snapshot.get('online', []) if e['demo'] == '3+']
    if online_main:
        e = min(online_main, key=lambda e: e['service_rank'])
        mentions.append(f"#{e['service_rank']} on {possessive(e['service'])} 14-day online list "
                        f"({num(e['viewers'])})")
    result['mentions'] = mentions

    # ---- week by week (ISO weeks ending on a Sunday on or before the newest data)
    history = []
    last_days = [parse_date(r['Date']) for r in tv_rows] + \
        [window_of(r)[1] for r in totaltv_rows + online_rows]
    if last_days:
        newest = max(last_days)
        sunday = newest + timedelta(days=6 - newest.weekday())
        first = max(since, min(last_days))
        monday = first - timedelta(days=first.weekday())
        while monday <= sunday:
            end = monday + timedelta(days=6)
            week = {'week': f'{monday.isocalendar()[0]}-W{monday.isocalendar()[1]:02d}',
                    'monday': monday.isoformat(), 'sunday': end.isoformat()}
            airings = [a for a in tv_rows if monday.isoformat() <= a['Date'] <= end.isoformat()]
            if airings:
                a = best_airing(airings)
                week['linear'] = {'airings': len(airings), 'best': airing_text(a),
                                  'viewers_7d': to_int(a['Viewers7d']),
                                  'viewers_same_day': to_int(a['ViewersSameDay']),
                                  'channel_rank': to_int(a['ChannelRank']), 'channel': a['Channel']}
            # 14-day lists whose window ends this Sunday (published on the Tuesday after)
            ends = [r for r in totaltv_rows if r['Demo'] == '3+' and window_of(r)[1] == end]
            if ends:
                r = min(ends, key=lambda r: r['Date'])
                week['totaltv'] = {'viewers': to_int(r['Viewers']), 'group_rank': to_int(r['GroupRank']),
                                   'episode': r['Episode'], 'list_date': r['Date']}
            ends = [r for r in online_rows if r['Demo'] == '3+' and r['Period'] == '14D' and window_of(r)[1] == end]
            if ends:
                first_date = min(r['Date'] for r in ends)
                r = max((r for r in ends if r['Date'] == first_date), key=lambda r: to_int(r['Viewers']))
                week['online'] = {'viewers': to_int(r['Viewers']), 'service': r['Service'],
                                  'service_rank': online_service_rank(online_all, r)}
            if len(week) > 3:
                history.append(week)
            monday += timedelta(days=7)
    result['weeks'] = history
    result['season'] = season(history, totaltv_rows)
    result['note'] = note(result)
    result['summary_note'] = summary_note(result)
    return result


def note(result):
    """One English line for Quickbase: short factual phrases, no em dashes. Only lists the series is on
    are mentioned; a series on none of them gets no note at all (empty string)."""
    snapshot = result['snapshot']
    if not snapshot:
        return ''
    parts = [f"Finnpanel {snapshot['window']}:"]
    tt = {e['demo']: e for e in snapshot['totaltv']}
    if '3+' in tt:
        e = tt['3+']
        parts.append(f"Total TV {num(e['viewers'])} avg viewers (3+, most-watched episode), "
                     f"#{e['group_rank']} on {possessive(GROUP_NAMES[e['group']])} Total TV top 20.")
        young = tt.get('Alle 45')
        if young:
            parts.append(f"Under 45: {num(young['viewers'])} (#{young['group_rank']}).")
    elif tt:
        e = min(tt.values(), key=lambda e: e['group_rank'])
        parts.append(f"Total TV {num(e['viewers'])} among {DEMO_LABELS[e['demo']]}, "
                     f"#{e['group_rank']} on {possessive(GROUP_NAMES[e['group']])} Total TV top 20.")
    if snapshot['linear']:
        a = best_airing(snapshot['linear'])
        rank = f", #{a['ChannelRank']} on {possessive(a['Channel'])} weekly top 20" if a['ChannelRank'] else ''
        parts.append(f"Best TV airing {airing_text(a)}{rank}.")
    online = [e for e in snapshot['online'] if e['demo'] == '3+']
    online90 = [e for e in snapshot.get('online90', []) if e['demo'] == '3+']
    if online:
        e = max(online, key=lambda e: e['viewers'])
        parts.append(f"Online {e['service']} {num(e['viewers'])} (#{e['service_rank']}).")
    elif online90:
        e = max(online90, key=lambda e: e['viewers'])
        parts.append(f"Online {e['service']} {num(e['viewers'])} over 90 days (#{e['service_rank']}).")
    return ' '.join(parts) if len(parts) > 1 else ''


def season(history, totaltv_rows):
    """Peaks over the weeks since --since: for a finished series, the figures worth keeping."""
    if not history:
        return {}
    summary = {'first_week': history[0]['week'], 'last_week': history[-1]['week'],
               'weeks_listed': len(history)}
    linear = [w for w in history if 'linear' in w]
    if linear:
        best = max(linear, key=lambda w: w['linear']['viewers_7d'] or w['linear']['viewers_same_day'] or 0)
        summary['best_linear'] = {'week': best['week'], **best['linear']}
    online = [w for w in history if 'online' in w]
    if online:
        best = max(online, key=lambda w: w['online']['viewers'])
        summary['best_online'] = {'week': best['week'], **best['online']}
    rows = [r for r in totaltv_rows if r['Demo'] == '3+']
    if rows:
        r = max(rows, key=lambda r: to_int(r['Viewers']))
        start, end = window_of(r)
        summary['best_totaltv'] = {'viewers': to_int(r['Viewers']), 'group_rank': to_int(r['GroupRank']),
                                   'group': r['Group'], 'window': window_text(start, end), 'episode': r['Episode']}
    return summary


def summary_note(result):
    """One English line for a series that has finished airing: its peaks."""
    s = result.get('season') or {}
    if not s:
        return ''
    first, last = s['first_week'][-2:], s['last_week'][-2:]
    weeks = f"wk {first}{'' if first == last else '-' + last}/{s['last_week'][:4]}"
    parts = [f'Finnpanel season peaks ({weeks}):']
    if 'best_totaltv' in s:
        t = s['best_totaltv']
        parts.append(f"Total TV {num(t['viewers'])} (3+, {t['window']}), "
                     f"#{t['group_rank']} on {possessive(GROUP_NAMES[t['group']])} Total TV top 20.")
    if 'best_linear' in s:
        b = s['best_linear']
        rank = f", #{b['channel_rank']} on {possessive(b['channel'])} weekly top 20" if b['channel_rank'] else ''
        parts.append(f"Best TV airing {b['best']}{rank}.")
    if 'best_online' in s:
        o = s['best_online']
        parts.append(f"Best online {o['service']} {num(o['viewers'])} (14 days, #{o['service_rank']}).")
    return ' '.join(parts)


# ---------------------------------------------------------------- output

def print_report(result, out=sys.stdout):
    w = out.write
    w(f"Series: {', '.join(result['names'])}   (channel group: {result['group_name'] or 'unknown'})\n")
    w('Matched names (check these are the right programme):\n')
    for kind, label in (('totaltv', 'Total TV'), ('online', 'Online'), ('tv', 'Linear')):
        w(f"  {label:9} {'; '.join(result['matched'][kind]) or '-'}\n")
    s = result['snapshot']
    if s:
        w(f"\nNewest lists: {s['list_date']} (window {s['window']})\n")
        w('  Total TV (TV + online, most-watched episode):\n')
        for e in s['totaltv']:
            w(f"    {e['demo']:8} {num(e['viewers']):>10}  #{e['group_rank']} on {GROUP_NAMES[e['group']]} top 20"
              f"  (#{e['rank']} of all three)  ep: {e['episode'] or '-'}\n")
        if not s['totaltv']:
            w('    -\n')
        w('  Online 14 days:\n')
        for e in s['online']:
            w(f"    {e['demo']:8} {num(e['viewers']):>10}  #{e['service_rank']} on {e['service']}\n")
        if not s['online']:
            w('    -\n')
        w('  Linear airings in the window:\n')
        for a in sorted(s['linear'], key=lambda a: (a['Date'], a['Time'])):
            w(f"    {airing_text(a)}  reach {num(to_int(a['Reach']))}, #{a['ChannelRank'] or '-'} on channel"
              + (f", top 50 #{a['Top50Rank']}" if a['Top50Rank'] else '')
              + (f", top 100 #{a['Top100Rank']}" if a['Top100Rank'] else '') + '\n')
        if not s['linear']:
            w('    -\n')
    w('\nMentions (best first):\n')
    for m in result['mentions'] or ['(none)']:
        w(f'  - {m}\n')
    w('\nWeek by week (3+):\n')
    w(f"  {'week':9} {'Total TV 14d':>14} {'online 14d':>12}  best TV airing\n")
    for week in result['weeks']:
        tt = week.get('totaltv')
        on = week.get('online')
        li = week.get('linear')
        w(f"  {week['week']:9} {(num(tt['viewers']) + ' #' + str(tt['group_rank'])) if tt else '-':>14} "
          f"{num(on['viewers']) if on else '-':>12}  "
          f"{(li['best'] + (' #' + str(li['channel_rank']) if li['channel_rank'] else '')) if li else '-'}\n")
    w(f"\nDashboard: {result['dashboard']}\n")
    # An empty note means the series is on no list: nothing is written about it
    w(f"\nQuickbase note (on air):\n{result['note'] or '(none: not on any list, write nothing)'}\n")
    w(f"\nQuickbase note (season finished):\n{result['summary_note'] or '(none: not on any list, write nothing)'}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('names', nargs='*', help='Finnpanel name(s) of the series, e.g. "Koko Suomi leipoo"')
    parser.add_argument('--search', help='list Finnpanel names containing this text, to find the right name')
    parser.add_argument('--exclude', action='append', default=[], help='a name that must not match')
    parser.add_argument('--since', type=date.fromisoformat, help='ignore data before this date (season start)')
    parser.add_argument('--as-of', type=date.fromisoformat, help='use lists up to this date (default today)')
    parser.add_argument('--group', choices=list(GROUP_NAMES), help='channel group, if it cannot be inferred')
    parser.add_argument('--data', help='local checkout or base URL (default: this checkout or GitHub)')
    parser.add_argument('--json', action='store_true', help='print JSON instead of text')
    args = parser.parse_args(argv)
    if not args.names and not args.search:
        parser.error('give a series name or --search')

    data = load(Source(args.data), args.since)
    if args.search:
        hits = search(data, args.search)
        if args.json:
            print(json.dumps([{**h, 'where': sorted(h['where'])} for h in hits], ensure_ascii=False, indent=1))
        for h in hits:
            print(f"{h['name']}  [{'; '.join(sorted(h['where']))}]  last {h['last']}, {h['rows']} rows"
                  + ('  (trailer)' if h['promo'] else ''))
        return 0
    result = analyse(data, args.names, args.exclude, args.since, args.as_of, args.group)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=1))
    else:
        print_report(result)
    return 0


if __name__ == '__main__':
    sys.exit(main())
