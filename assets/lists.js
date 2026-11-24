// The top-list views shared by the Online and Total TV tabs: the table and the demo comparison.
// Both read state.rows, one day's list in a common shape:
// {Date, Demo, Rank, Service, Program, Episode, Duration, Viewers}. TotalTV rows also carry
// GroupRank, PeriodStart, PeriodEnd and, once enriched (totaltv.js), their online and linear figures.

const state = {
    source: 'online',
    view: {online: 'list', totaltv: 'chart'},
    rows: [],
    comparison: [],
    hasDemoData: false,
    openShowKey: null,
    compareSort: 'total',
    showAllRows: false,
};

const topRows = 20;

function selectedService() {
    return document.getElementById('service').value;
}

function selectedDemo() {
    return document.getElementById('demo').value || '3+';
}

function inService(serviceName) {
    const service = selectedService();
    return service === 'all' || groupKeyOf(serviceName) === service;
}

function demoSortKey(demo) {
    const index = DEMO_ORDER.indexOf(demo);
    return index === -1 ? DEMO_ORDER.length : index;
}

// Offer only the demos present in the loaded list, keeping the current choice when possible
function updateDemoOptions() {
    const select = document.getElementById('demo');
    const previous = select.value || '3+';
    const available = [...new Set(state.rows.map(row => row.Demo))]
        .sort((a, b) => demoSortKey(a) - demoSortKey(b));
    select.innerHTML = '';
    available.forEach(demo => select.add(new Option(`Demo: ${demo}`, demo)));
    select.value = previous;
    if (!select.value && available.length > 0) {
        select.value = available.includes('3+') ? '3+' : available[0];
    }
    state.hasDemoData = available.some(demo => demo !== '3+');
}

// ---------------------------------------------------------------- table

function programCell(row) {
    const td = el('td');
    td.appendChild(link(titleHref(row.Program), String(row.Program || ''), 'title-link'));
    return td;
}

function updateTable() {
    const demo = selectedDemo();
    const rows = state.rows.filter(row => row.Demo === demo && inService(row.Service));
    const totaltv = state.source === 'totaltv';
    const showEpisode = totaltv || rows.some(row => row.Episode);
    const columns = totaltv
        ? ['Rank', 'Broadcaster', 'Program', 'Episode', 'Duration', 'Viewers', 'Online, same 14 days', 'Online share', 'Best linear airing']
        : ['Date', 'Demo', 'Rank', 'Service', 'Program', ...(showEpisode ? ['Episode'] : []), 'Duration', 'Viewers'];

    const head = document.getElementById('tableHead');
    head.innerHTML = '';
    const headRow = el('tr');
    columns.forEach(column => headRow.appendChild(el('th', /Viewers|Online|Rank/.test(column) ? 'num' : null, column)));
    head.appendChild(headRow);

    const body = document.getElementById('tableBody');
    body.innerHTML = '';
    rows.forEach(row => {
        const tr = el('tr');
        const viewers = row.Viewers ? fullNumber(parseInt(row.Viewers, 10)) : '';
        if (totaltv) {
            const online = row.online;
            tr.append(
                el('td', 'num', String(row.Rank)),
                el('td', null, `${row.Service} #${row.GroupRank}`),
                programCell(row),
                el('td', 'episode-cell', row.Episode || ''),
                el('td', null, row.Duration || ''),
                el('td', 'num', viewers),
                el('td', 'num', online ? `${fullNumber(online.viewers)} (#${online.rank})` : (row.onlineCeiling ? `≤ ${fullNumber(row.onlineCeiling)}` : '–')),
                el('td', 'num', row.onlineShare !== null && row.onlineShare !== undefined ? `${row.onlineShareApprox ? '≈' : ''}${percent(row.onlineShare)}` : '–'),
                el('td', null, row.bestAiring ? airingText(row.bestAiring) : '–'),
            );
        } else {
            tr.append(el('td', null, row.Date || ''), el('td', null, row.Demo || ''), el('td', 'num', String(row.Rank || '')),
                el('td', null, row.Service || ''), programCell(row));
            if (showEpisode) tr.appendChild(el('td', 'episode-cell', row.Episode || ''));
            tr.append(el('td', null, row.Duration || ''), el('td', 'num', viewers));
        }
        body.appendChild(tr);
    });
    if (!rows.length) {
        const tr = el('tr');
        const td = el('td', 'empty-cell', 'No programmes in this list.');
        td.colSpan = columns.length;
        tr.appendChild(td);
        body.appendChild(tr);
    }
}

function airingText(airing) {
    const viewers = airing.Viewers7d || airing.ViewersSameDay;
    return `${fullNumber(viewers)} · ${airing.Channel} ${WEEKDAY_NAMES[airing.Weekday] || airing.Weekday} ${finnishDate(airing.Date, false)} ${airing.Time}`;
}

// ---------------------------------------------------------------- demo comparison

// One entry per show with its figures in each demo. Alle 45 and 45+ together make up the
// 3+ audience, so a figure missing from one of those lists can be worked out from the other two
function buildComparison(rows) {
    const listFloor = {};
    const shows = new Map();
    rows.forEach(row => {
        const viewers = Number(row.Viewers);
        const rank = Number(row.Rank);
        if (!viewers) return;
        const listKey = `${row.Demo}|${row.Service}`;
        listFloor[listKey] = Math.min(listFloor[listKey] ?? Infinity, viewers);
        const name = seriesName(row.Program);
        const key = `${row.Service}|${titleKey(row.Program)}`;
        if (!shows.has(key)) {
            shows.set(key, {key, name, program: row.Program, service: row.Service, demos: {}});
        }
        const show = shows.get(key);
        const current = show.demos[row.Demo];
        if (!current || rank < current.rank) {
            show.demos[row.Demo] = {viewers, rank, program: String(row.Program), groupRank: row.GroupRank ? Number(row.GroupRank) : null};
        }
    });

    return [...shows.values()].map(show => {
        const values = {};
        DEMO_ORDER.forEach(demo => {
            values[demo] = show.demos[demo] ? show.demos[demo].viewers : null;
        });
        const total = values['3+'];
        const under = values['Alle 45'];
        const over = values['45+'];
        const derived = {};
        if (total !== null && under === null && over !== null) {
            values['Alle 45'] = Math.max(total - over, 0);
            derived['Alle 45'] = '3+ minus 45+';
        }
        if (total !== null && over === null && under !== null) {
            values['45+'] = Math.max(total - under, 0);
            derived['45+'] = '3+ minus Alle 45';
        }
        if (total === null && under !== null && over !== null) {
            values['3+'] = under + over;
            derived['3+'] = 'Alle 45 plus 45+';
        }
        // Each list shows the show's most-watched episode in that demo; when those episodes
        // differ, the two age groups no longer add up to 3+
        const mismatch = total !== null && under !== null && over !== null
            && Math.abs(under + over - total) > Math.max(2000, total * 0.05);
        const u = values['Alle 45'];
        const o = values['45+'];
        const split = u !== null && o !== null && u + o > 0 ? u / (u + o) : null;
        const share2564 = values['3+'] && values['25-64'] !== null
            ? Math.min(values['25-64'] / values['3+'], 1) : null;
        const ceilings = {};
        DEMO_ORDER.forEach(demo => {
            ceilings[demo] = listFloor[`${demo}|${show.service}`] ?? null;
        });
        return {
            ...show, values, derived, mismatch, split, share2564, ceilings,
            splitApprox: split !== null && (mismatch || 'Alle 45' in derived || '45+' in derived || '3+' in derived),
        };
    });
}

function notListedText(show, demo) {
    const ceiling = show.ceilings[demo];
    return ceiling ? `at most ${fullNumber(ceiling)}` : 'not known';
}

function rankShade(rank) {
    return rank <= 10 ? 'top' : rank <= 30 ? 'mid' : 'low';
}

function sortShows(shows, sort) {
    const value = (show, demo) => show.values[demo] ?? -1;
    const byTotal = (a, b) => value(b, '3+') - value(a, '3+');
    const bySplit = direction => (a, b) => {
        if (a.split === null || b.split === null) {
            return (a.split === null) - (b.split === null) || byTotal(a, b);
        }
        return direction * (a.split - b.split) || byTotal(a, b);
    };
    const comparers = {
        total: byTotal,
        under45: (a, b) => value(b, 'Alle 45') - value(a, 'Alle 45') || byTotal(a, b),
        over45: (a, b) => value(b, '45+') - value(a, '45+') || byTotal(a, b),
        young: bySplit(-1),
        old: bySplit(1),
    };
    return shows.sort(comparers[sort]);
}

function shownShows() {
    const includeOutside = document.getElementById('includeOutside').checked;
    return sortShows(state.comparison.filter(show => inService(show.service)
        && (includeOutside || show.demos['3+'])), state.compareSort);
}

function updateCompare() {
    const message = document.getElementById('compareMessage');
    const content = document.getElementById('compareContent');
    if (!state.hasDemoData) {
        message.textContent = state.rows.length
            ? 'This date only has the 3+ demo. Demo data starts on 2026-10-07, so pick that date or later.'
            : 'No data loaded for this date.';
        message.hidden = false;
        content.hidden = true;
        return;
    }
    message.hidden = true;
    content.hidden = false;

    const shows = shownShows();
    // One scale for both sides so under-45 and 45+ bars compare directly
    const scale = Math.max(1, ...shows.map(show =>
        Math.max(show.values['Alle 45'] || 0, show.values['45+'] || 0)));
    const list = document.getElementById('pyramid');
    list.innerHTML = '';
    (state.showAllRows ? shows : shows.slice(0, topRows)).forEach((show, index) => {
        list.appendChild(renderShowRow(show, scale, index));
        if (show.key === state.openShowKey) {
            list.appendChild(detailItem(show));
        }
    });

    const more = document.getElementById('showAll');
    more.hidden = shows.length <= topRows;
    more.textContent = state.showAllRows ? `Show top ${topRows}` : `Show all ${shows.length} shows`;
}

function splitTrack(split, className) {
    const track = el('span', className);
    [['seg-under45', split], ['seg-over45', 1 - split]].forEach(([segClass, share]) => {
        if (share <= 0) return;
        const seg = el('span', `track-seg ${segClass}`);
        seg.style.flex = `${share} 1 0`;
        track.appendChild(seg);
    });
    return track;
}

// A row is a grid: rank, the name (a link to the title page), and the bars. The bars are the
// button that opens the detail card; a click anywhere else on the row (but the link) does too.
function renderShowRow(show, scale, index) {
    const li = el('li');
    li.dataset.key = show.key;
    const row = el('div', 'row-btn');
    row.addEventListener('click', event => {
        if (!event.target.closest('a, button')) toggleDetail(show.key);
    });

    const overall = show.demos['3+'];
    const pos = el('span', overall ? 'pos' : 'pos none', overall ? String(overall.rank) : '–');
    pos.dataset.tip = overall ? `#${overall.rank} among all viewers (3+)` : 'Not in the 3+ list';

    const meta = [show.service];
    if (show.values['3+'] !== null) {
        meta.push(`${'3+' in show.derived ? '≈' : ''}${thousands(show.values['3+'])} viewers`);
    }
    if (show.split !== null) {
        meta.push(`${show.splitApprox ? '≈' : ''}${percent(show.split)} under 45`);
    }
    const who = el('span', 'who');
    who.append(link(titleHref(show.program), show.name, 'name title-link'), el('span', 'meta', meta.join(' · ')));

    const fly = el('button', 'fly fly-btn');
    fly.type = 'button';
    fly.setAttribute('aria-expanded', String(show.key === state.openShowKey));
    fly.setAttribute('aria-label', `Details for ${show.name}`);
    fly.addEventListener('click', () => toggleDetail(show.key));
    fly.append(renderHalf(show, 'Alle 45', 'left', scale, index), el('span', 'axis'),
        renderHalf(show, '45+', 'right', scale, index));
    row.append(pos, who, fly);
    li.appendChild(row);
    return li;
}

function renderHalf(show, demo, side, scale, index) {
    const half = el('span', `half ${side}`);
    const group = demo === 'Alle 45' ? 'Under 45' : '45+';
    const value = show.values[demo];
    const entry = show.demos[demo];
    const val = el('span', 'val');
    if (value === null) {
        const ceiling = show.ceilings[demo];
        val.classList.add('missing');
        val.textContent = ceiling ? `≤${thousands(ceiling)}` : '–';
        half.dataset.tip = `${group}: not in the ${demo} list (${notListedText(show, demo)} viewers)`;
        half.appendChild(val);
        return half;
    }
    const bar = el('span', entry ? 'bar' : 'bar approx');
    bar.style.setProperty('--w', String(value / scale));
    bar.style.setProperty('--delay', `${Math.min(index, topRows) * 25}ms`);
    const amount = el('span', null, `${entry ? '' : '≈'}${thousands(value)}`);
    if (entry) {
        const chip = el('b', `rk ${rankShade(entry.rank)}`, `#${entry.rank}`);
        side === 'left' ? val.append(amount, chip) : val.append(chip, amount);
        half.dataset.tip = `${group}: ${fullNumber(entry.viewers)} viewers · #${entry.rank} in ${demo}`;
    } else {
        val.appendChild(amount);
        half.dataset.tip = `${group}: ≈${fullNumber(value)} viewers, worked out as ${show.derived[demo]}`;
    }
    side === 'left' ? half.append(val, bar) : half.append(bar, val);
    return half;
}

function toggleDetail(key) {
    const list = document.getElementById('pyramid');
    const open = list.querySelector('.detail-item');
    if (open) open.remove();
    list.querySelectorAll('.fly-btn[aria-expanded="true"]').forEach(button => button.setAttribute('aria-expanded', 'false'));
    state.openShowKey = state.openShowKey === key ? null : key;
    const row = [...list.children].find(li => li.dataset.key === key);
    const show = state.comparison.find(item => item.key === key);
    if (!state.openShowKey || !row || !show) return;
    row.after(detailItem(show));
    row.querySelector('.fly-btn').setAttribute('aria-expanded', 'true');
}

function detailItem(show) {
    const li = el('li', 'detail-item');
    li.appendChild(renderDetail(show, {titleLink: true}));
    return li;
}

// The demo card: total, the under-45/45+ split and one rank tile per demo. Also used on title pages.
function renderDetail(show, {titleLink = false, heading = null} = {}) {
    const card = el('div', 'detail');

    // Ruutu lists name the episode; say it once when every list shows the same one
    const programs = [...new Set(Object.values(show.demos).map(entry => entry.program))];
    const sharedEpisode = programs.length === 1 && programs[0] !== show.name ? programs[0] : null;

    const top = el('div', 'detail-top');
    const title = el('div');
    const h3 = el('h3');
    if (titleLink) {
        h3.appendChild(link(titleHref(show.program), heading || show.name, 'title-link'));
    } else {
        h3.textContent = heading || show.name;
    }
    title.append(h3,
        el('div', 'detail-meta', sharedEpisode ? `${show.service} · Most-watched episode: ${sharedEpisode}` : show.service));
    if (titleLink) {
        title.appendChild(link(titleHref(show.program), 'Open title history →', 'detail-open'));
    }
    const hero = el('div', 'hero');
    if (show.values['3+'] !== null) {
        hero.append(el('span', 'hero-num', `${'3+' in show.derived ? '≈' : ''}${fullNumber(show.values['3+'])}`),
            el('span', 'hero-label', 'viewers aged 3+'));
    } else {
        hero.append(el('span', 'hero-label', `Not in the 3+ list (${notListedText(show, '3+')} viewers)`));
    }
    top.append(title, hero);
    card.appendChild(top);

    if (show.split !== null) {
        const approx = show.splitApprox ? '≈' : '';
        const split = el('div', 'split-big');
        const left = el('span', 'pct', approx + percent(show.split));
        left.appendChild(el('small', null, 'under 45'));
        const right = el('span', 'pct right', approx + percent(1 - show.split));
        right.appendChild(el('small', null, '45 or older'));
        split.append(left, splitTrack(show.split, 'split-track'), right);
        card.appendChild(split);
    } else {
        card.appendChild(el('p', 'detail-note', 'Not enough data to split this audience by age.'));
    }

    const ranks = Object.values(show.demos).map(entry => entry.rank);
    const bestRank = ranks.length ? Math.min(...ranks) : null;
    const tiles = el('div', 'rank-tiles');
    const tileClass = {'Alle 45': 'under45', '45+': 'over45'};
    DEMO_ORDER.forEach(demo => {
        const entry = show.demos[demo];
        const value = show.values[demo];
        const tile = el('div', `rank-tile ${tileClass[demo] || ''}`);
        tile.append(el('span', 'big', entry ? `#${entry.rank}` : '–'), el('span', 'lbl', DEMO_LABELS[demo]));
        if (entry) {
            tile.appendChild(el('span', 'sub', `${fullNumber(entry.viewers)} viewers`));
            if (entry.groupRank) {
                tile.appendChild(el('span', 'episode', `#${entry.groupRank} at ${show.service}`));
            }
            if (!sharedEpisode && entry.program !== show.name) {
                tile.appendChild(el('span', 'episode', entry.program));
            }
            if (entry.rank === bestRank && ranks.length > 1) {
                tile.appendChild(el('span', 'badge', 'Best'));
            }
        } else if (value !== null) {
            tile.appendChild(el('span', 'sub', `≈${fullNumber(value)} viewers (${show.derived[demo]})`));
        } else {
            tile.appendChild(el('span', 'sub', `Not in the list: ${notListedText(show, demo)}`));
        }
        tiles.appendChild(tile);
    });
    card.appendChild(tiles);

    const notes = [];
    if (show.share2564 !== null) {
        notes.push(`${'3+' in show.derived ? '≈' : ''}${percent(show.share2564)} of the viewers are 25–64.`);
    }
    if (show.mismatch) {
        notes.push('The 3+, Alle 45 and 45+ lists show different episodes of this show, so the split is approximate.');
    } else if (Object.keys(show.derived).length > 0) {
        notes.push('≈ figures are worked out from the other groups: under 45 and 45+ together make up the 3+ audience.');
    }
    if (notes.length) {
        card.appendChild(el('p', 'detail-note', notes.join(' ')));
    }
    return card;
}
