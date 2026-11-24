"""Checks for series_ratings.py on a small fixture shaped like data/.

Run: python -m unittest discover tests
"""
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import series_ratings as sr  # noqa: E402

TOTALTV = """Date,PeriodStart,PeriodEnd,Demo,Population,Rank,GroupRank,Group,Program,Episode,Duration,Viewers
2026-11-24,2026-11-09,2026-11-22,3+,5472000,1,1,MTV,Tanssii Tähtien Kanssa,Finaali,1:27:00,1023000
2026-11-24,2026-11-09,2026-11-22,3+,5472000,25,12,MTV,Koko Suomi leipoo,Sielunruokaa,0:41:00,488000
2026-11-24,2026-11-09,2026-11-22,3+,5472000,40,20,MTV,Huomenta Suomi,,0:30:00,300000
2026-11-24,2026-11-09,2026-11-22,Alle 45,2777000,12,7,MTV,Koko Suomi leipoo,Sielunruokaa,0:41:00,134000
2026-11-24,2026-11-09,2026-11-22,3+,5472000,30,15,Sanoma,Syke - Kausi 24 - Jakso 4 12.11.2026 06:00,,0:45:00,400000
2026-11-24,2026-11-09,2026-11-22,3+,5472000,45,20,Sanoma,Uutiset,,0:20:00,200000
"""
ONLINE = """Date,Period,Demo,Rank,Service,Program,Episode,Duration,Viewers,ServiceRank,PeriodStart,PeriodEnd,Population
2026-11-17,14D,3+,20,MTV Katsomo,Koko Suomi leipoo,,0:41:00,64000,,,,
2026-11-17,14D,3+,10,MTV Katsomo,Salatut elämät,,0:25:00,90000,,,,
2026-11-24,14D,3+,27,MTV Katsomo,Koko Suomi leipoo,,0:41:00,67000,9,2026-11-09,2026-11-22,5472000
2026-11-24,14D,3+,50,Ruutu,Pimeä Puoli: Riku Rantala & Kale Puonti - Kausi 1,,0:21:10,39000,30,2026-11-09,2026-11-22,5472000
"""
TV = """Year,Week,Date,Weekday,Time,Channel,Group,Program,Reach,ViewersSameDay,Viewers7d,ChannelRank,Top100Rank,Top50Rank,Fetched
2026,46,2026-11-10,ti,19:59,MTV3,MTV,Koko Suomi leipoo,606000,267000,290000,15,,35,2026-11-24
2026,46,2026-11-11,ke,21:05,MTV Ava,MTV,Koko Suomi leipoo,57000,21000,26000,9,,,2026-11-24
2026,47,2026-11-16,ma,19:00,MTV Ava,MTV,Koko Britannia leipoo: jouluerikoinen,137000,31000,,7,,,2026-11-24
2026,47,2026-11-17,ti,20:00,MTV3,MTV,Koko Suomi leipoo,514000,278000,,16,,37,2026-11-24
2026,47,2026-11-19,to,22:00,Nelonen,Sanoma,Tulossa: Pimeä Puoli: Riku Rantala & Kal,42000,32000,,16,,,2026-11-24
"""


class SeriesRatingsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        for folder, name, text in (('totaltv', '2026-11.csv', TOTALTV), ('online', '2026-11.csv', ONLINE),
                                   ('tv', '2026.csv', TV)):
            (root / 'data' / folder).mkdir(parents=True)
            (root / 'data' / folder / name).write_text(text, encoding='utf-8')
        manifest = {kind: {'files': [f'data/{kind}/{name}']} for kind, name in
                    (('totaltv', '2026-11.csv'), ('online', '2026-11.csv'), ('tv', '2026.csv'))}
        (root / 'data' / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        cls.data = sr.load(sr.Source(str(root)), None)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def result(self, *names, **kwargs):
        return sr.analyse(self.data, list(names), as_of=date(2026, 11, 24), **kwargs)

    def test_title_key_matches_dashboard_rules(self):
        self.assertEqual(sr.title_key('Syke - Kausi 24 - Jakso 4 12.11.2026 06:00'), 'syke')
        self.assertEqual(sr.title_key('Valamiehet (12)'), 'valamiehet')
        self.assertEqual(len(sr.title_key('x' * 60)), 40)

    def test_matching_skips_other_shows_and_trailers(self):
        match = sr.matcher(['Pimeä Puoli'])
        self.assertTrue(match('Pimeä Puoli: Riku Rantala & Kale Puonti - Kausi 1'))
        self.assertFalse(match('Tulossa: Pimeä Puoli: Riku Rantala & Kal'))
        leipoo = sr.matcher(['Koko Suomi leipoo'])
        self.assertFalse(leipoo('Koko Britannia leipoo: jouluerikoinen'))
        self.assertFalse(sr.matcher(['Koti koiralle'])('Koti koiralle UK'))

    def test_snapshot_and_mentions(self):
        result = self.result('Koko Suomi leipoo')
        snapshot = result['snapshot']
        self.assertEqual(snapshot['window'], '9-22 Nov 2026')
        self.assertEqual([(e['demo'], e['viewers'], e['group_rank']) for e in snapshot['totaltv']],
                         [('3+', 488000, 12), ('Alle 45', 134000, 7)])
        self.assertEqual(len(snapshot['linear']), 3)  # the British special is not counted
        self.assertEqual(result['mentions'][0],
                         "#12 on MTV's Total TV top 20 (3+, 9-22 Nov 2026), 488,000 viewers")
        self.assertIn("#7 on MTV's Total TV top 20 among under 45 (134,000)", result['mentions'])
        # Channel rank of the most-watched airing (MTV3), not the best rank on a small channel
        self.assertIn("#15 on MTV3's weekly top 20 (week 46)", result['mentions'])
        self.assertNotIn('—', result['note'])
        self.assertTrue(result['note'].startswith('Finnpanel 9-22 Nov 2026: Total TV 488,000'))

    def test_weeks(self):
        weeks = {w['week']: w for w in self.result('Koko Suomi leipoo')['weeks']}
        self.assertEqual(weeks['2026-W46']['linear']['viewers_7d'], 290000)
        self.assertEqual(weeks['2026-W47']['totaltv']['viewers'], 488000)
        self.assertEqual(weeks['2026-W47']['online']['viewers'], 67000)
        # Archive row without ServiceRank: position by viewers within MTV Katsomo's list
        self.assertEqual(weeks['2026-W46']['online']['service_rank'], 2)  # list of 17 Nov, window to 15 Nov

    def test_lists_the_series_is_not_on_are_not_mentioned(self):
        result = self.result('Pimeä Puoli')
        self.assertEqual(result['group'], 'Sanoma')
        self.assertEqual(result['matched']['tv'], [])
        self.assertEqual(result['note'], 'Finnpanel 9-22 Nov 2026: Online Ruutu 39,000 (#30).')
        for word in ('Not ', 'cut-off', 'Total TV', 'TV airing'):
            self.assertNotIn(word, result['note'])

    def test_series_on_no_list_gets_no_note(self):
        result = self.result('Ei Tällaista Sarjaa')
        self.assertEqual(result['note'], '')
        self.assertEqual(result['summary_note'], '')
        self.assertEqual(result['mentions'], [])

    def test_since_drops_older_lists(self):
        weeks = self.result('Koko Suomi leipoo', since=date(2026, 11, 16))['weeks']
        self.assertEqual([w['week'] for w in weeks], ['2026-W47'])


if __name__ == '__main__':
    unittest.main()
