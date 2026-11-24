"""Parser checks for FPTV.py against the page formats Finnpanel served in November 2026.

Run: python -m unittest discover tests
"""
import csv
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import FPTV  # noqa: E402


def totaltv_page(service='MTV', description='TV + online, viimeiset 14 päivää, ohjelman katsotuin jakso',
                 demo='Kaikki 3 vuotta täyttäneet'):
    return f"""<html><body><table class="totaltv"><tbody>
<tr><td class="reportheader" colspan="5"><div id="report_header">
<div><strong>{service}</strong></div>
<div><strong>{description}</strong></div>
<div>Ajanjakso: <strong>9.11.2026 – 22.11.2026</strong></div>
<div>Kohderyhmä: <strong>{demo}</strong></div>
<div>Väestö: <strong>5 472 000</strong></div>
</div></td></tr>
<tr><th>#</th><th>Ohjelma</th><th>Jakso</th><th>Kesto</th><th class="sort_column">Keskikatsojamäärä</th></tr>
<tr><th class="hl">1.</th><td class="hl">Tanssii Tähtien Kanssa</td><td class="hl">Finaalipaikat jaossa!</td><td class="hl">1:27:00</td><td class="sort_column">1 023 000</td></tr>
<tr><th>2.</th><td>Masked Singer Suomi</td><td></td><td>1:07:00</td><td class="sort_column">750&nbsp;000</td></tr>
</tbody></table></body></html>"""


WEEK_TOP100 = """﻿Viikon 100 katsotuinta lähetystä (kaikki kanavat, kaikki esityskerrat)

viikko 39/2026

#;Ohjelma;Kanava;Päivä;Kello;Tavoittavuus;Keski- katsoja- määrä esitys- päivänä;Keski- katsoja- määrä 7 vrk:n aikana
1;Tanssii Tähtien Kanssa;MTV3;su;19:27;1398000;932000;980000
49;Jeopardy! Suomi;TV Viisi;to;20:00;339000;203000;213000
"""

WEEK_CHANNEL = """﻿Viikon katsotuimmat lähetykset kanavittain
TV5
viikko 39/2026

#;Ohjelma;Päivä;Kello;Tavoittavuus;Keski- katsoja- määrä esitys- päivänä;Keski- katsoja- määrä 7 vrk:n aikana
1;Jeopardy! Suomi;to;20:00;339000;203000;213000
"""

WEEK_CHANNEL_MTV3 = """﻿Viikon katsotuimmat lähetykset kanavittain
MTV3
viikko 39/2026

#;Ohjelma;Päivä;Kello;Tavoittavuus;Keski- katsoja- määrä esitys- päivänä
1;Tanssii Tähtien Kanssa;su;19:27;1398000;932000
"""


class FakeFetcher:
    def __init__(self, pages):
        self.pages = pages

    def get(self, path):
        return self.pages[path]


class TopListTest(unittest.TestCase):
    def test_parses_rows_and_header(self):
        rows = FPTV.parse_top_list(totaltv_page(), 'totaltv', '14D', 'mtv', '3plus')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['Program'], 'Tanssii Tähtien Kanssa')
        self.assertEqual(rows[0]['Episode'], 'Finaalipaikat jaossa!')
        self.assertEqual(rows[0]['Viewers'], 1023000)
        self.assertEqual(rows[1]['Viewers'], 750000)
        self.assertEqual(rows[0]['PeriodStart'], '2026-11-09')
        self.assertEqual(rows[0]['PeriodEnd'], '2026-11-22')
        self.assertEqual(rows[0]['Population'], 5472000)
        self.assertEqual((rows[0]['Group'], rows[0]['Demo'], rows[0]['GroupRank']), ('MTV', '3+', 1))

    def test_online_list(self):
        page = totaltv_page('MTV Katsomo', 'Viimeiset 14 päivää, ohjelman katsotuin jakso')
        rows = FPTV.parse_top_list(page, 'online', '14D', 'mtv', '3plus')
        self.assertEqual((rows[0]['Service'], rows[0]['ServiceRank']), ('MTV Katsomo', 1))

    def test_refuses_the_online_list_finnpanel_serves_for_unknown_urls(self):
        page = totaltv_page('MTV Katsomo', 'Viimeiset 90 päivää, ohjelman katsotuin jakso')
        with self.assertRaises(ValueError):
            FPTV.parse_top_list(page, 'totaltv', '14D', 'mtv', '3plus')
        with self.assertRaises(ValueError):
            FPTV.parse_top_list(page, 'online', '14D', 'mtv', '3plus')

    def test_refuses_total_tv_as_online_and_wrong_service(self):
        with self.assertRaises(ValueError):
            FPTV.parse_top_list(totaltv_page('MTV Katsomo'), 'online', '14D', 'mtv', '3plus')
        with self.assertRaises(ValueError):
            FPTV.parse_top_list(totaltv_page('Yle'), 'totaltv', '14D', 'mtv', '3plus')

    def test_refuses_another_demo(self):
        with self.assertRaises(ValueError):
            FPTV.parse_top_list(totaltv_page(), 'totaltv', '14D', 'mtv', 'alle45')

    def test_month_file_ranks_across_groups_and_rerun_replaces_the_day(self):
        pages = {}
        for key, name in FPTV.GROUPS.items():
            for demo_key, demo_name in FPTV.DEMO_NAMES.items():
                page = totaltv_page(name, demo=demo_name)
                if key == 'yle':  # Yle's top programme is bigger than MTV's
                    page = page.replace('1 023 000', '1 100 000')
                pages[f'/tulokset/totaltv/{key}/total14/{demo_key}.html'] = page
        original = FPTV.DATA
        with tempfile.TemporaryDirectory() as folder:
            FPTV.DATA = Path(folder)
            try:
                for _ in range(2):
                    self.assertEqual(FPTV.collect_top_lists(FakeFetcher(pages), date(2026, 11, 24), 'totaltv'), 0)
                with (Path(folder) / 'totaltv' / '2026-11.csv').open(encoding='utf-8') as handle:
                    rows = list(csv.DictReader(handle))
            finally:
                FPTV.DATA = original
        self.assertEqual(len(rows), 3 * 4 * 2, 'a rerun replaces the day instead of adding to it')
        first = [row for row in rows if row['Demo'] == '3+'][:2]
        self.assertEqual([(row['Rank'], row['Group'], row['GroupRank']) for row in first], [('1', 'Yle', '1'), ('2', 'MTV', '1')])


class WeeklyListTest(unittest.TestCase):
    def test_columns_are_found_by_header(self):
        channel, rows = FPTV.parse_week_csv(WEEK_CHANNEL)
        self.assertEqual(channel, 'TV5')
        self.assertEqual(rows[0]['channel'], 'TV5')
        self.assertEqual((rows[0]['day'], rows[0]['time']), ('to', '20:00'))
        self.assertEqual((rows[0]['reach'], rows[0]['same_day'], rows[0]['seven_day']), (339000, 203000, 213000))
        _, rows = FPTV.parse_week_csv(WEEK_TOP100)
        self.assertEqual(rows[1]['channel'], 'TV Viisi')
        _, rows = FPTV.parse_week_csv(WEEK_CHANNEL_MTV3)
        self.assertIsNone(rows[0]['seven_day'])  # the newest week has no 7-day figure yet

    def test_week_merges_lists_and_channel_aliases(self):
        base = '/tulokset/tv/vko'
        fetcher = FakeFetcher({
            f'{base}/top/2026/39/top.html': f'<a href="{base}/top/2026/39/viisi.html"></a><a href="{base}/top/2026/39/mtv3.html"></a>',
            f'{base}/top/2026/39/viisi.csv': WEEK_CHANNEL,
            f'{base}/top/2026/39/mtv3.csv': WEEK_CHANNEL_MTV3,
            f'{base}/top100/2026/39/index.csv': WEEK_TOP100,
            f'{base}/top50p/2026/39/index.csv': WEEK_TOP100,
        })
        rows = FPTV.collect_week(fetcher, 2026, 39, date(2026, 11, 24))
        self.assertEqual(len(rows), 2, 'TV Viisi in the top 100 is TV5 in its channel list')
        jeopardy = next(row for row in rows if row['Program'] == 'Jeopardy! Suomi')
        self.assertEqual((jeopardy['Channel'], jeopardy['Date']), ('TV5', '2026-09-24'))
        self.assertEqual((jeopardy['ChannelRank'], jeopardy['Top100Rank']), (1, 49))
        dance = next(row for row in rows if row['Channel'] == 'MTV3')
        self.assertEqual(dance['Group'], 'MTV')
        self.assertEqual(dance['Viewers7d'], 980000, '7-day figure taken from the list that has it')
        self.assertEqual(dance['Date'], '2026-09-27')

    def test_week_back_crosses_years(self):
        self.assertEqual(FPTV.week_back(2026, 47, 9), (2026, 38))
        self.assertEqual(FPTV.week_back(2027, 2, 3), (2026, 52))  # 2026 has 53 ISO weeks


if __name__ == '__main__':
    unittest.main()
