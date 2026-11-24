// The list views shared by the Total TV and Verkkopalvelut tabs: the table and the demo comparison
// (the top list is in toplist.js). All read state.rows, one day's list in a common shape:
// {Date, Period, Demo, Rank, Service, Program, Episode, Duration, Viewers}. Total TV rows also carry
// GroupRank, PeriodStart, PeriodEnd and, once enriched (toplist.js), their online and linear figures.

const state = {
    source: 'totaltv',
    view: 'top',
    filters: {date: '', period: '14D', service: 'all', demo: '3+'},
    rows: [],
    shownDate: null,
    comparison: [],
    demosAvailable: ['3+'],
    openShowKey: null,
    compareSort: 'total',
    showAllRows: false,
};

const topRows = 20;

function selectedDemo() {
    return state.filters.demo;
}

function inService(serviceName) {
    return state.filters.service === 'all' || groupKeyOf(serviceName) === state.filters.service;
}

function demoSortKey(demo) {
    const index = DEMO_ORDER.indexOf(demo);
    return index === -1 ? DEMO_ORDER.length : index;
}

function openTitle(program) {
    location.hash = titleHref(program);
}

// ---------------------------------------------------------------- table

function updateTable() {
    const demo = selectedDemo();
    const rows = state.rows.filter(row => row.Demo === demo && inService(row.Service));
    const totaltv = state.source === 'totaltv';
    const columns = totaltv
        ? [['Sija', 'num'], ['Mediatalo', ''], ['Ohjelma', ''], ['Jakso', ''], ['Kesto', ''], ['Katsojia', 'num'],
            ['Verkossa samana aikana', 'num'], ['Verkon osuus', 'num'], ['Paras tv-lähetys', '']]
        : [['Sija', 'num'], ['Palvelu', ''], ['Ohjelma', ''], ['Jakso', ''], ['Kesto', ''], ['Katsojia', 'num']];

    const head = document.getElementById('tableHead');
    head.innerHTML = '';
    const headRow = el('tr');
    columns.forEach(([column, className]) => headRow.appendChild(el('th', className || null, column)));
    head.appendChild(headRow);

    const body = document.getElementById('tableBody');
    body.innerHTML = '';
    rows.forEach(row => {
        const tr = el('tr', 'link-row');
        tr.tabIndex = 0;
        tr.title = `Avaa ${seriesName(row.Program)}: koko historia`;
        tr.addEventListener('click', event => {
            if (!event.target.closest('a')) openTitle(row.Program);
        });
        tr.addEventListener('keydown', event => {
            if (event.key === 'Enter') openTitle(row.Program);
        });
        const name = el('td');
        name.appendChild(link(titleHref(row.Program), String(row.Program || ''), 'title-link'));
        const rank = el('td', 'num', `${row.Rank}.`);
        rank.title = 'Sija kaikkien kolmen listalla';
        const service = totaltv ? `${row.Service} (${row.GroupRank}.)` : `${row.Service} (${row.ServiceRank}.)`;
        tr.append(rank, el('td', null, service), name, el('td', 'episode-cell', row.Episode || ''),
            el('td', null, row.Duration || ''), el('td', 'num', fullNumber(row.Viewers)));
        if (totaltv) {
            const online = row.online;
            tr.append(
                el('td', 'num', online ? fullNumber(online.viewers) : (row.onlineCeiling ? `enint. ${fullNumber(row.onlineCeiling)}` : '–')),
                el('td', 'num', row.onlineShare !== null && row.onlineShare !== undefined ? `${row.onlineShareApprox ? '≈' : ''}${percent(row.onlineShare)}` : '–'),
                el('td', null, row.bestAiring ? airingText(row.bestAiring) : '–'),
            );
        }
        body.appendChild(tr);
    });
    if (!rows.length) {
        const tr = el('tr');
        const td = el('td', 'empty-cell', 'Tällä valinnalla ei ole ohjelmia.');
        td.colSpan = columns.length;
        tr.appendChild(td);
        body.appendChild(tr);
    }
}

function airingText(airing) {
    const viewers = airing.Viewers7d || airing.ViewersSameDay;
    return `${fullNumber(viewers)} · ${airing.Channel} ${WEEKDAY_NAMES[airing.Weekday] || airing.Weekday} ${finnishDate(airing.Date, false)} klo ${airing.Time}`;
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
            const ownRank = row.GroupRank || row.ServiceRank;
            show.demos[row.Demo] = {viewers, rank, program: String(row.Program), ownRank: ownRank ? Number(ownRank) : null};
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
            derived['Alle 45'] = 'kaikki katsojat miinus 45+';
        }
        if (total !== null && over === null && under !== null) {
            values['45+'] = Math.max(total - under, 0);
            derived['45+'] = 'kaikki katsojat miinus alle 45';
        }
        if (total === null && under !== null && over !== null) {
            values['3+'] = under + over;
            derived['3+'] = 'alle 45 plus 45+';
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
    return ceiling ? `enintään ${fullNumber(ceiling)}` : 'ei tiedossa';
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
    if (state.demosAvailable.length < 2) {
        message.textContent = state.rows.length
            ? 'Tältä päivältä on vain kaikkien katsojien (3+) lista. Kohderyhmät kerätään 7.10.2026 alkaen.'
            : 'Tälle päivälle ei ole listaa.';
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
    more.textContent = state.showAllRows ? `Näytä kärki ${topRows}` : `Näytä kaikki ${shows.length} ohjelmaa`;
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
    pos.dataset.tip = overall ? `${overall.rank}. kaikkien katsojien (3+) listalla` : 'Ei kaikkien katsojien listalla';

    const meta = [show.service];
    if (show.values['3+'] !== null) {
        meta.push(`${'3+' in show.derived ? '≈' : ''}${thousands(show.values['3+'])} katsojaa`);
    }
    if (show.split !== null) {
        meta.push(`${show.splitApprox ? '≈' : ''}${percent(show.split)} alle 45-v.`);
    }
    const who = el('span', 'who');
    who.append(link(titleHref(show.program), show.name, 'name title-link'), el('span', 'meta', meta.join(' · ')));

    const fly = el('button', 'fly fly-btn');
    fly.type = 'button';
    fly.setAttribute('aria-expanded', String(show.key === state.openShowKey));
    fly.setAttribute('aria-label', `${show.name}: kohderyhmät tarkemmin`);
    fly.addEventListener('click', () => toggleDetail(show.key));
    fly.append(renderHalf(show, 'Alle 45', 'left', scale, index), el('span', 'axis'),
        renderHalf(show, '45+', 'right', scale, index));
    row.append(pos, who, fly);
    li.appendChild(row);
    return li;
}

function renderHalf(show, demo, side, scale, index) {
    const half = el('span', `half ${side}`);
    const group = demo === 'Alle 45' ? 'Alle 45-vuotiaat' : '45 vuotta täyttäneet';
    const value = show.values[demo];
    const entry = show.demos[demo];
    const val = el('span', 'val');
    if (value === null) {
        const ceiling = show.ceilings[demo];
        val.classList.add('missing');
        val.textContent = ceiling ? `≤${thousands(ceiling)}` : '–';
        half.dataset.tip = `${group}: ei listalla (${notListedText(show, demo)} katsojaa)`;
        half.appendChild(val);
        return half;
    }
    const bar = el('span', entry ? 'bar' : 'bar approx');
    bar.style.setProperty('--w', String(value / scale));
    bar.style.setProperty('--delay', `${Math.min(index, topRows) * 25}ms`);
    const amount = el('span', null, `${entry ? '' : '≈'}${thousands(value)}`);
    if (entry) {
        const chip = el('b', `rk ${rankShade(entry.rank)}`, `${entry.rank}.`);
        side === 'left' ? val.append(amount, chip) : val.append(chip, amount);
        half.dataset.tip = `${group}: ${fullNumber(entry.viewers)} katsojaa · ${entry.rank}. ryhmän listalla`;
    } else {
        val.appendChild(amount);
        half.dataset.tip = `${group}: ≈${fullNumber(value)} katsojaa, laskettu: ${show.derived[demo]}`;
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
        el('div', 'detail-meta', sharedEpisode ? `${show.service} · katsotuin jakso: ${sharedEpisode}` : show.service));
    if (titleLink) {
        title.appendChild(link(titleHref(show.program), 'Avaa ohjelman koko historia →', 'detail-open'));
    }
    const hero = el('div', 'hero');
    if (show.values['3+'] !== null) {
        hero.append(el('span', 'hero-num', `${'3+' in show.derived ? '≈' : ''}${fullNumber(show.values['3+'])}`),
            el('span', 'hero-label', 'katsojaa, kaikki 3+'));
    } else {
        hero.append(el('span', 'hero-label', `Ei kaikkien katsojien listalla (${notListedText(show, '3+')} katsojaa)`));
    }
    top.append(title, hero);
    card.appendChild(top);

    if (show.split !== null) {
        const approx = show.splitApprox ? '≈' : '';
        const split = el('div', 'split-big');
        const left = el('span', 'pct', approx + percent(show.split));
        left.appendChild(el('small', null, 'alle 45-vuotiaita'));
        const right = el('span', 'pct right', approx + percent(1 - show.split));
        right.appendChild(el('small', null, '45 vuotta täyttäneitä'));
        split.append(left, splitTrack(show.split, 'split-track'), right);
        card.appendChild(split);
    } else {
        card.appendChild(el('p', 'detail-note', 'Yleisöä ei voi jakaa ikäryhmiin näillä tiedoilla.'));
    }

    const ranks = Object.values(show.demos).map(entry => entry.rank);
    const bestRank = ranks.length ? Math.min(...ranks) : null;
    const tiles = el('div', 'rank-tiles');
    const tileClass = {'Alle 45': 'under45', '45+': 'over45'};
    DEMO_ORDER.forEach(demo => {
        const entry = show.demos[demo];
        const value = show.values[demo];
        const tile = el('div', `rank-tile ${tileClass[demo] || ''}`);
        tile.append(el('span', 'big', entry ? `${entry.rank}.` : '–'), el('span', 'lbl', DEMO_LABELS[demo]));
        if (entry) {
            tile.appendChild(el('span', 'sub', `${fullNumber(entry.viewers)} katsojaa`));
            if (entry.ownRank) {
                tile.appendChild(el('span', 'episode', `${entry.ownRank}. ${genitive(show.service)} listalla`));
            }
            if (!sharedEpisode && entry.program !== show.name) {
                tile.appendChild(el('span', 'episode', entry.program));
            }
            if (entry.rank === bestRank && ranks.length > 1) {
                tile.appendChild(el('span', 'badge', 'Paras'));
            }
        } else if (value !== null) {
            tile.appendChild(el('span', 'sub', `≈${fullNumber(value)} katsojaa (${show.derived[demo]})`));
        } else {
            tile.appendChild(el('span', 'sub', `Ei listalla: ${notListedText(show, demo)}`));
        }
        tiles.appendChild(tile);
    });
    card.appendChild(tiles);

    const notes = [];
    if (show.share2564 !== null) {
        notes.push(`${'3+' in show.derived ? '≈' : ''}${percent(show.share2564)} katsojista on 25–64-vuotiaita.`);
    }
    if (show.mismatch) {
        notes.push('Kohderyhmien listoilla on ohjelman eri jaksoja, joten jako on likimääräinen.');
    } else if (Object.keys(show.derived).length > 0) {
        notes.push('≈-luvut on laskettu muista ryhmistä: alle 45-vuotiaat ja 45 vuotta täyttäneet ovat yhdessä kaikki katsojat.');
    }
    if (notes.length) {
        card.appendChild(el('p', 'detail-note', notes.join(' ')));
    }
    return card;
}
