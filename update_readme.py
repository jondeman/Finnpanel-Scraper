"""Refresh only the generated block in README.md.

The old workflow did `echo ... > README.md`, which destroyed anything a human
wrote there. This replaces just the span between the two markers and leaves the
rest of the file alone.
"""

import glob
import os
import re
from datetime import datetime, timezone

import pandas as pd

README = 'README.md'
BEGIN, END = '<!-- BEGIN:generated -->', '<!-- END:generated -->'
ARCHIVE = 'data/finnpanel_all.csv'


def build_block():
    per_day = sorted(glob.glob('data/*/*.csv') + glob.glob('data/*/*.xlsx'))
    lines = [BEGIN,
             f'_Päivitetty: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC_',
             '']

    if os.path.exists(ARCHIVE):
        df = pd.read_csv(ARCHIVE, usecols=['Date', 'Period'])
        dates = df['Date'].astype(str)
        lines += [
            '| | |',
            '|---|---|',
            f'| Aineisto | {dates.min()} – {dates.max()} |',
            f'| Päiviä | {dates.nunique():,} |',
            f'| Rivejä | {len(df):,} |',
            f'| Päivätiedostoja | {len(per_day):,} |',
            f'| Koottu arkisto | [`{ARCHIVE}`]({ARCHIVE}) '
            f'({os.path.getsize(ARCHIVE) / 1048576:.1f} MB) |',
            '',
        ]

    newest = [p for p in per_day if p.endswith('.csv')][-2:]
    if newest:
        lines.append('Viimeisimmät tiedostot:')
        lines += [f'- [`{p}`]({p})' for p in newest]
        lines.append('')

    lines.append(END)
    return '\n'.join(lines)


def main():
    block = build_block()
    if os.path.exists(README):
        text = open(README, encoding='utf-8').read()
    else:
        text = ''

    if BEGIN in text and END in text:
        text = re.sub(re.escape(BEGIN) + r'.*?' + re.escape(END),
                      lambda _: block, text, flags=re.S)
    else:
        # First run, or a human removed the markers: append rather than clobber.
        text = (text.rstrip() + '\n\n' + block + '\n') if text else block + '\n'

    open(README, 'w', encoding='utf-8').write(text)
    print(f'README.md updated ({len(block)} bytes in generated block)')


if __name__ == '__main__':
    main()
