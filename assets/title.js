// Title page: everything known about one programme. Total TV (TV + online, from 2026-11-24),
// the online lists (Yle Areena, MTV Katsomo, Ruutu: 3+ since 2024-09-02, every demo since
// 2026-10-07) and its linear airings (the weekly broadcast lists Finnpanel keeps, 10 weeks back).

const SOURCE_COLORS = {totaltv: '#6250d6', online: '#1baf7a', tv: '#eda100'};
const titleState = {key: null, demo: '3+', range: 'all', window: '14D', mix: null, dataTab: 'tv', showAll: {}};
const RANGE_DAYS = {'90': 90, '365': 365, all: null};

function byDateBest(rows) {
    // One point per list date: the best entry (several Ruutu episodes of one series can make a list)
    const best = new Map();
    rows.forEach(row => {
        const current = best.get(row.Date);
        if (!current || Number(row.Viewers) > Number(current.Viewers)) best.set(row.Date, row);
    });
    return [...best.values()].sort((a, b) => a.Date.localeCompare(b.Date));
}

function airingValue(airing) {
    return Number(airing.Viewers7d || airing.ViewersSameDay);
}

function segmented(options, value, onChange, label) {
    const group = el('div', 'segmented');
    group.setAttribute('role', 'group');
    group.setAttribute('aria-label', label);
    options.forEach(([optionValue, text, disabled]) => {
        const button = el('button', null, text);
        button.type = 'button';
        button.setAttribute('aria-pressed', String(optionValue === value));
        if (disabled) {
            button.disabled = true;
            button.title = disabled;
        }
        button.addEventListener('click', () => onChange(optionValue));
        group.appendChild(button);
    });
    return group;
}

async function renderTitlePage(key) {
    const page = document.getElementById('titlePage');
    if (titleState.key !== key) {
        Object.assign(titleState, {key, demo: '3+', range: 'all', window: '14D', mix: null, dataTab: 'tv', showAll: {}});
        window.scrollTo(0, 0);
    }
    if (!page.dataset.key || page.dataset.key !== key) {
        page.innerHTML = '';
        page.appendChild(el('p', 'title-loading', 'Loading the title history…'));
    }
    page.dataset.key = key;
    let history;
    try {
        history = await loadHistory();
    } catch (error) {
        page.innerHTML = '';
        page.appendChild(el('p', 'title-loading', `Could not load the history files: ${error.message}`));
        return;
    }
    if (titleState.key !== key) return; // another title was opened meanwhile
    const title = history.titles.get(key);
    page.innerHTML = '';
    page.appendChild(link(lastListRoute, '← Back to the lists', 'back-link'));
    if (!title) {
        page.appendChild(el('h2', 'title-name', 'Title not found'));
        page.appendChild(el('p', null, 'No list has a programme by this name. Try the search above.'));
        return;
    }
    document.title = `${title.name} · Finnpanel Dashboard`;
    drawTitle(page, title, history);
}

function drawTitle(page, title, history) {
    const demo = titleState.demo;
    const onlineRows = title.online.filter(row => row.Period === titleState.window);
    const totaltvByDemo = byDateBest(title.totaltv.filter(row => row.Demo === demo));
    const onlineByDemo = byDateBest(onlineRows.filter(row => row.Demo === demo));
    const airings = [...title.tv].sort((a, b) => a.Date.localeCompare(b.Date) || a.Time.localeCompare(b.Time));

    // ---- header
    const services = [...new Set([...title.totaltv.map(row => row.Group), ...title.online.map(row => row.Service)])];
    const channels = [...new Set(title.tv.map(row => row.Channel))];
    const header = el('div', 'title-header');
    header.appendChild(el('h2', 'title-name', title.name));
    const facts = [];
    if (services.length) facts.push(services.join(', '));
    if (channels.length) facts.push(`on ${channels.join(', ')}`);
    header.appendChild(el('p', 'title-facts', facts.join(' · ')));
    const coverage = el('ul', 'coverage');
    const since = dates => dates.length ? `collected since ${finnishDate(dates[0])}` : 'not collected yet';
    const weeks = history.manifest.tv.weeks || [];
    const kept = weeks.length ? `weeks ${weeks[0].slice(-2)}–${weeks[weeks.length - 1].slice(-2)}/${weeks[weeks.length - 1].slice(0, 4)} kept` : 'not collected yet';
    const onlineDates = [...new Set(title.online.filter(row => row.Period === '14D').map(row => row.Date))].sort();
    const totaltvDates = [...new Set(title.totaltv.map(row => row.Date))].sort();
    coverage.append(
        coverageItem('totaltv', 'Total TV', totaltvDates.length
            ? `${totaltvDates.length} of ${history.listDates.totaltv.length} daily lists` : 'not in any list yet',
        since(history.listDates.totaltv)),
        coverageItem('online', 'Online top 20', onlineDates.length
            ? `${onlineDates.length} daily 14-day lists, ${finnishDate(onlineDates[0])}–${finnishDate(onlineDates[onlineDates.length - 1])}` : 'not in any list',
        since(history.listDates.online)),
        coverageItem('tv', 'Linear TV', airings.length
            ? `${airings.length} airings in the weekly lists` : 'no airing in the weekly lists',
        kept),
    );
    header.appendChild(coverage);
    page.appendChild(header);

    // ---- controls that scope everything below
    const redraw = () => drawTitleAgain(title, history);
    const controls = el('div', 'title-controls');
    const demosWithData = new Set([...title.totaltv, ...title.online].map(row => row.Demo));
    controls.append(
        el('span', 'toolbar-label', 'Demo'),
        segmented(DEMO_ORDER.map(option => [option, option === '3+' ? '3+' : DEMO_LABELS[option],
            demosWithData.has(option) ? null : 'Not in any list for this demo']), demo, value => { titleState.demo = value; redraw(); }, 'Demo'),
        el('span', 'toolbar-label', 'Range'),
        segmented([['90', '3 months'], ['365', '1 year'], ['all', 'All']], titleState.range, value => { titleState.range = value; redraw(); }, 'Date range'),
        el('span', 'toolbar-label', 'Online window'),
        segmented([['14D', '14 days'], ['90D', '90 days']], titleState.window, value => { titleState.window = value; redraw(); }, 'Online list window'),
    );
    page.appendChild(controls);

    // ---- headline
    page.appendChild(renderHeadline(title, history, totaltvByDemo, onlineByDemo, airings));

    // ---- time range
    const lastDate = [history.listDates.online.slice(-1)[0], history.listDates.totaltv.slice(-1)[0]].filter(Boolean).sort().slice(-1)[0];
    const to = parseIso(addDays(lastDate, -2)).getTime();
    const firstPoint = [
        ...totaltvByDemo.map(row => row.PeriodEnd),
        ...onlineByDemo.map(row => rowWindow(row).end),
        ...(demo === '3+' ? airings.map(row => row.Date) : []),
    ].sort()[0];
    const rangeDays = RANGE_DAYS[titleState.range];
    let from = firstPoint ? parseIso(firstPoint).getTime() - 3 * DAY_MS : to - 90 * DAY_MS;
    if (rangeDays) from = Math.max(from, to - rangeDays * DAY_MS);
    from = Math.min(from, to - 14 * DAY_MS);

    // ---- chart 1: the audience over time
    const windowLabel = titleState.window === '14D' ? '14 days' : '90 days';
    const viewersCard = card('Average viewers over time',
        `${DEMO_LABELS[demo]}. Each list gives the programme's most-watched episode in its window, plotted at the window's last day; the line breaks where the programme was out of the top 20.${demo === '3+' ? ' Dots are single linear airings (7-day figure when published).' : ' Linear airings are measured for all viewers (3+) only.'}`);
    const chart = el('div', 'chart-box');
    viewersCard.appendChild(chart);
    page.appendChild(viewersCard);
    const series = [
        {id: 'totaltv', label: 'Total TV: TV + online, 14 days', color: SOURCE_COLORS.totaltv, kind: 'line', gapDays: 2,
            points: totaltvByDemo.map(row => ({t: parseIso(row.PeriodEnd).getTime(), y: Number(row.Viewers),
                lines: [`#${row.Rank} overall, #${row.GroupRank} at ${row.Group}`, `window ${windowText(row.PeriodStart, row.PeriodEnd)}`, row.Episode].filter(Boolean)}))},
        {id: 'online', label: `Online only, ${windowLabel}`, color: SOURCE_COLORS.online, kind: 'line', gapDays: 2,
            points: onlineByDemo.map(row => {
                const window = rowWindow(row);
                const episode = ruutuEpisode(row.Program);
                return {t: parseIso(window.end).getTime(), y: Number(row.Viewers),
                    lines: [`#${row.ServiceRank} on ${row.Service}`, `window ${windowText(window.start, window.end)}`,
                        row.Episode || (episode ? episodeLabel(episode) : '')].filter(Boolean)};
            })},
    ];
    if (demo === '3+') {
        series.push({id: 'tv', label: 'Linear airings', color: SOURCE_COLORS.tv, kind: 'dots',
            points: airings.map(row => ({t: parseIso(row.Date).getTime(), y: airingValue(row),
                lines: [`${row.Channel} ${WEEKDAY_NAMES[row.Weekday] || row.Weekday} ${row.Time}`, `reach ${fullNumber(row.Reach)}`,
                    row.Viewers7d ? `7-day figure (same day ${fullNumber(row.ViewersSameDay)})` : 'same-day figure; 7-day not published yet']}))});
    }
    const drawCharts = [() => renderTimeChart(chart, series, {from, to, height: 300,
        describe: `Average viewers of ${title.name} over time`, emptyText: 'No figures for this demo in this range.'})];

    // ---- chart 2: under 45 vs 45+
    const mixCard = card('Under 45 and 45+ over time',
        'The two age groups add up to the 3+ audience. A gap means the programme was out of that group\'s top 20. Demo lists start on 2026-10-07 (online) and 2026-11-24 (Total TV).');
    const mixSources = [['totaltv', 'Total TV'], ['online', `Online, ${windowLabel}`]];
    if (!titleState.mix) {
        // Open on whichever source has more demo history for this title
        const demoDays = rows => new Set(rows.filter(row => row.Demo === 'Alle 45' || row.Demo === '45+').map(row => row.Date)).size;
        titleState.mix = demoDays(title.totaltv) >= demoDays(onlineRows) ? 'totaltv' : 'online';
    }
    mixCard.querySelector('.card-head').appendChild(segmented(mixSources, titleState.mix, value => { titleState.mix = value; redraw(); }, 'Source'));
    const mixChart = el('div', 'chart-box');
    mixCard.appendChild(mixChart);
    page.appendChild(mixCard);
    const mixRows = titleState.mix === 'totaltv' ? title.totaltv : onlineRows;
    const mixSeries = [['Alle 45', 'Under 45', '#2a78d6'], ['45+', '45+', '#eb6834']].map(([demoName, label, color]) => ({
        id: demoName, label, color, kind: 'line', gapDays: 2,
        points: byDateBest(mixRows.filter(row => row.Demo === demoName)).map(row => {
            return {t: parseIso(rowWindow(row).end).getTime(), y: Number(row.Viewers), lines: [`#${row.GroupRank || row.ServiceRank} at ${row.Service}`]};
        }),
    }));
    // Demo lists are recent, so this chart starts where its own figures start rather than leaving
    // most of its width empty
    const mixFirst = Math.min(...mixSeries.flatMap(item => item.points.map(point => point.t)).filter(t => t >= from));
    const mixFrom = Number.isFinite(mixFirst) ? Math.max(from, Math.min(mixFirst - 3 * DAY_MS, to - 14 * DAY_MS)) : from;
    drawCharts.push(() => renderTimeChart(mixChart, mixSeries, {from: mixFrom, to, height: 240,
        describe: `Under 45 and 45+ viewers of ${title.name}`, emptyText: 'Not in the under 45 or 45+ lists in this range.'}));

    // ---- latest demo breakdown, one card per source
    const breakdown = el('div', 'breakdown');
    const latestTotal = latestComparison(history.totaltv, title.key, totaltvDates);
    if (latestTotal) {
        breakdown.appendChild(renderDetail(latestTotal.show, {heading: `Total TV, ${windowText(latestTotal.rows[0].PeriodStart, latestTotal.rows[0].PeriodEnd)}`}));
    }
    const demoOnlineDates = [...new Set(title.online.filter(row => row.Period === '14D' && row.Demo !== '3+').map(row => row.Date))].sort();
    const latestOnline = latestComparison(history.online.filter(row => row.Period === '14D'), title.key, demoOnlineDates);
    if (latestOnline) {
        const window = rowWindow(latestOnline.rows[0]);
        breakdown.appendChild(renderDetail(latestOnline.show, {heading: `Online only, ${windowText(window.start, window.end)}`}));
    }
    if (breakdown.children.length) {
        const breakdownCard = card('Latest list by demo', 'The most recent list with the programme in it. Rank is across Yle, MTV and Sanoma; dark chips are top 10.');
        breakdownCard.appendChild(breakdown);
        page.appendChild(breakdownCard);
    }

    // ---- linear airings
    if (airings.length) {
        const airingsCard = card('Linear airings', `Every airing in Finnpanel's weekly lists: each channel's top 20, the overall top 100 and the top 50 by best airing. All viewers (3+). Finnpanel keeps ${ARCHIVE_WEEKS_TEXT}; the dashboard keeps everything collected since.`);
        airingsCard.appendChild(table(
            ['Date', 'Time', 'Channel', 'As listed', 'Reach', 'Avg viewers, same day', 'Avg viewers, 7 days', 'Rank on channel', 'Top 100'],
            [...airings].reverse().map(row => [
                `${WEEKDAY_NAMES[row.Weekday] || row.Weekday} ${finnishDate(row.Date)}`, row.Time, row.Channel, row.Program,
                num(row.Reach), num(row.ViewersSameDay), num(row.Viewers7d), row.ChannelRank ? `#${row.ChannelRank}` : '', row.Top100Rank ? `#${row.Top100Rank}` : '',
            ]), 'airings', 20));
        page.appendChild(airingsCard);
    }

    // ---- episodes
    const episodes = episodeRows(title);
    if (episodes.length) {
        const episodesCard = card('Most-watched episodes', 'Episodes the lists named, with each one\'s best figure among all viewers (3+). Episode names from the online lists were not saved before this release; Ruutu names carry the season and episode.');
        episodesCard.appendChild(table(['Episode', 'Source', 'Best', 'In lists'],
            episodes.map(item => [item.episode, item.source, fullNumber(item.best), item.first === item.last ? finnishDate(item.first) : `${finnishDate(item.first)}–${finnishDate(item.last)}`]),
            'episodes', 15));
        page.appendChild(episodesCard);
    }

    // ---- all rows
    page.appendChild(renderDataCard(title));
    drawCharts.forEach(draw => draw());
    titleCharts = drawCharts;
}

const ARCHIVE_WEEKS_TEXT = 'only the last 10 weeks';
let titleCharts = [];

function drawTitleAgain(title, history) {
    const page = document.getElementById('titlePage');
    const scroll = window.scrollY;
    page.innerHTML = '';
    page.appendChild(link(lastListRoute, '← Back to the lists', 'back-link'));
    drawTitle(page, title, history);
    window.scrollTo(0, scroll);
}

function coverageItem(source, label, text, note) {
    const item = el('li');
    const key = el('i', source === 'tv' ? 'key-dot' : 'key-line');
    key.style.background = SOURCE_COLORS[source];
    const body = el('span');
    body.append(el('strong', null, label), document.createTextNode(` ${text}`), el('small', null, note));
    item.append(key, body);
    return item;
}

function card(heading, note) {
    const section = el('section', 'card');
    const head = el('div', 'card-head');
    const text = el('div');
    text.append(el('h3', null, heading));
    if (note) text.appendChild(el('p', 'card-note', note));
    head.appendChild(text);
    section.appendChild(head);
    return section;
}

function num(value) {
    return value ? fullNumber(value) : '';
}

function episodeLabel(episode) {
    return [`Season ${episode.season}`, episode.episode ? `episode ${episode.episode}` : null,
        episode.published ? `published ${episode.published}` : null].filter(Boolean).join(', ');
}

function renderHeadline(title, history, totaltvByDemo, onlineByDemo, airings) {
    const wrap = el('div', 'headline');
    const demo = titleState.demo;
    const latestTotalDate = history.listDates.totaltv.slice(-1)[0];
    const latestOnlineDate = history.listDates.online.slice(-1)[0];

    const hero = el('div', 'hero-block');
    const lastTotal = totaltvByDemo[totaltvByDemo.length - 1];
    const lastOnline = onlineByDemo[onlineByDemo.length - 1];
    if (lastTotal) {
        hero.append(el('span', 'hero-num', fullNumber(lastTotal.Viewers)),
            el('span', 'hero-label', `average viewers, TV + online, ${windowText(lastTotal.PeriodStart, lastTotal.PeriodEnd)} (${DEMO_SHORT[demo]})`),
            el('span', 'hero-sub', `#${lastTotal.Rank} across Yle, MTV and Sanoma · #${lastTotal.GroupRank} at ${lastTotal.Group}${lastTotal.Date !== latestTotalDate ? ` · last in the list on ${finnishDate(lastTotal.Date)}` : ''}`));
    } else if (lastOnline) {
        const window = rowWindow(lastOnline);
        hero.append(el('span', 'hero-num', fullNumber(lastOnline.Viewers)),
            el('span', 'hero-label', `average viewers online, ${windowText(window.start, window.end)} (${DEMO_SHORT[demo]})`),
            el('span', 'hero-sub', `#${lastOnline.ServiceRank} on ${lastOnline.Service}${lastOnline.Date !== latestOnlineDate ? ` · last in the list on ${finnishDate(lastOnline.Date)}` : ''} · not in a Total TV list`));
    } else if (airings.length) {
        const best = airings.reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
        hero.append(el('span', 'hero-num', fullNumber(airingValue(best))),
            el('span', 'hero-label', 'average viewers, best linear airing (3+)'),
            el('span', 'hero-sub', `${best.Channel} ${WEEKDAY_NAMES[best.Weekday]} ${finnishDate(best.Date)} ${best.Time} · not in the online or Total TV top 20s`));
    } else {
        hero.append(el('span', 'hero-label', `Not in any ${DEMO_LABELS[demo]} list.`));
    }
    wrap.appendChild(hero);

    const tiles = el('div', 'stat-tiles');
    if (lastOnline && lastTotal) {
        const sameWindow = byDateBest(title.online.filter(row => row.Period === '14D' && row.Demo === demo))
            .find(row => row.Date === lastTotal.Date);
        tiles.appendChild(statTile('Online, same 14 days',
            sameWindow ? fullNumber(sameWindow.Viewers) : '–',
            sameWindow ? `${percent(Math.min(sameWindow.Viewers / lastTotal.Viewers, 1))} of Total TV · #${sameWindow.ServiceRank} on ${sameWindow.Service}` : 'not in the online top 20 that day'));
    } else if (lastOnline) {
        tiles.appendChild(statTile(`Online, ${titleState.window === '14D' ? '14' : '90'} days`, fullNumber(lastOnline.Viewers),
            `#${lastOnline.ServiceRank} on ${lastOnline.Service}, list of ${finnishDate(lastOnline.Date)}`));
    }
    if (onlineByDemo.length) {
        const peak = onlineByDemo.reduce((a, b) => Number(b.Viewers) > Number(a.Viewers) ? b : a);
        const window = rowWindow(peak);
        tiles.appendChild(statTile(`Online peak, ${titleState.window === '14D' ? '14' : '90'} days`, fullNumber(peak.Viewers),
            `window ${windowText(window.start, window.end)}`));
    }
    if (airings.length) {
        const lastWeek = airings[airings.length - 1].Week;
        const best = airings.filter(row => row.Week === lastWeek).reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
        tiles.appendChild(statTile(`Best airing, week ${lastWeek}`, fullNumber(airingValue(best)),
            `${best.Channel} ${WEEKDAY_NAMES[best.Weekday]} ${finnishDate(best.Date, false)} ${best.Time} · reach ${compact(Number(best.Reach))}`));
        if (lastTotal) {
            const inWindow = airings.filter(row => row.Date >= lastTotal.PeriodStart && row.Date <= lastTotal.PeriodEnd);
            if (inWindow.length) {
                const top = inWindow.reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
                const ratio = Number(lastTotal.Viewers) / airingValue(top);
                tiles.appendChild(statTile('Total TV vs best airing', `×${ratio.toFixed(2)}`,
                    `Total TV ${compact(Number(lastTotal.Viewers))} vs ${compact(airingValue(top))} on ${top.Channel} in the same 14 days (3+)`));
            }
        }
    }
    if (tiles.children.length) wrap.appendChild(tiles);
    return wrap;
}

function statTile(label, value, note) {
    const tile = el('div', 'stat-tile');
    tile.append(el('span', 'stat-label', label), el('span', 'stat-value', value), el('span', 'stat-note', note));
    return tile;
}

// The demo card for the latest list the title is in, built the same way as Compare demos
function latestComparison(rows, key, dates) {
    for (let i = dates.length - 1; i >= 0; i--) {
        const date = dates[i];
        const dayRows = rows.filter(row => row.Date === date);
        const shows = buildComparison(dayRows).filter(show => show.key.endsWith(`|${key}`));
        if (shows.length) {
            const show = shows.sort((a, b) => (b.values['3+'] || 0) - (a.values['3+'] || 0))[0];
            return {date, rows: dayRows, show};
        }
    }
    return null;
}

function episodeRows(title) {
    const episodes = new Map();
    const add = (episode, source, row) => {
        if (!episode) return;
        const id = `${source}|${episode}`;
        const item = episodes.get(id) || {episode, source, best: 0, first: row.Date, last: row.Date};
        item.best = Math.max(item.best, Number(row.Viewers));
        if (row.Date < item.first) item.first = row.Date;
        if (row.Date > item.last) item.last = row.Date;
        episodes.set(id, item);
    };
    title.totaltv.filter(row => row.Demo === '3+').forEach(row => {
        const parsed = ruutuEpisode(row.Program);
        add(row.Episode || (parsed ? episodeLabel(parsed) : ''), `Total TV, ${row.Group}`, row);
    });
    title.online.filter(row => row.Demo === '3+' && row.Period === '14D').forEach(row => {
        const parsed = ruutuEpisode(row.Program);
        add(row.Episode || (parsed ? episodeLabel(parsed) : ''), row.Service, row);
    });
    return [...episodes.values()].sort((a, b) => b.best - a.best);
}

function table(columns, rows, id, limit) {
    const wrap = el('div', 'table-wrap');
    const tableEl = el('table', 'data-table');
    const head = el('thead');
    const headRow = el('tr');
    columns.forEach(column => headRow.appendChild(el('th', null, column)));
    head.appendChild(headRow);
    const body = el('tbody');
    const showAll = titleState.showAll[id];
    (showAll ? rows : rows.slice(0, limit)).forEach(cells => {
        const tr = el('tr');
        cells.forEach(cell => tr.appendChild(el('td', /^[\d,#×≈≤–]/.test(String(cell)) ? 'num' : null, cell)));
        body.appendChild(tr);
    });
    tableEl.append(head, body);
    wrap.appendChild(tableEl);
    if (rows.length > limit) {
        const more = el('button', 'show-all', showAll ? `Show the first ${limit}` : `Show all ${rows.length} rows`);
        more.type = 'button';
        more.addEventListener('click', () => {
            titleState.showAll[id] = !showAll;
            wrap.replaceWith(table(columns, rows, id, limit));
        });
        wrap.appendChild(more);
    }
    return wrap;
}

function renderDataCard(title) {
    const section = card('All figures', 'Every list entry for this programme, newest first. The download has all three sources as separate sheets.');
    const download = el('button', 'download-btn', 'Download all (xlsx)');
    download.type = 'button';
    download.addEventListener('click', () => downloadSheets(`${title.name.replace(/[\\/:*?"<>|]/g, '')}_Finnpanel.xlsx`, [
        {name: 'Total TV', rows: title.totaltv.map(({Service, ...row}) => row)},
        {name: 'Online', rows: title.online},
        {name: 'Linear TV', rows: title.tv},
    ]));
    section.querySelector('.card-head').appendChild(download);
    const tabs = [
        ['tv', `Linear TV (${title.tv.length})`],
        ['totaltv', `Total TV (${title.totaltv.length})`],
        ['online', `Online (${title.online.length})`],
    ];
    const body = el('div');
    const draw = () => {
        body.innerHTML = '';
        const tab = titleState.dataTab;
        if (tab === 'tv') {
            body.appendChild(table(['Week', 'Date', 'Time', 'Channel', 'Program', 'Reach', 'Same day', '7 days'],
                [...title.tv].sort((a, b) => b.Date.localeCompare(a.Date)).map(row => [`${row.Week}/${row.Year}`, row.Date, row.Time, row.Channel, row.Program, num(row.Reach), num(row.ViewersSameDay), num(row.Viewers7d)]),
                'data-tv', 30));
        } else if (tab === 'totaltv') {
            body.appendChild(table(['List date', 'Window', 'Demo', 'Rank', 'Group rank', 'Program', 'Episode', 'Viewers'],
                [...title.totaltv].sort((a, b) => b.Date.localeCompare(a.Date) || demoSortKey(a.Demo) - demoSortKey(b.Demo)).map(row => [row.Date, windowText(row.PeriodStart, row.PeriodEnd), row.Demo, `#${row.Rank}`, `#${row.GroupRank} ${row.Group}`, row.Program, row.Episode, num(row.Viewers)]),
                'data-totaltv', 30));
        } else {
            body.appendChild(table(['List date', 'Window', 'Demo', 'Rank', 'Service rank', 'Program', 'Episode', 'Viewers'],
                [...title.online].sort((a, b) => b.Date.localeCompare(a.Date) || a.Period.localeCompare(b.Period) || demoSortKey(a.Demo) - demoSortKey(b.Demo)).map(row => {
                    const window = rowWindow(row);
                    return [row.Date, `${row.Period === '14D' ? '14' : '90'} d, ${windowText(window.start, window.end)}`, row.Demo, `#${row.Rank}`, `#${row.ServiceRank} ${row.Service}`, row.Program, row.Episode, num(row.Viewers)];
                }),
                'data-online', 30));
        }
    };
    const tabBar = segmented(tabs, titleState.dataTab, value => {
        titleState.dataTab = value;
        tabBar.querySelectorAll('button').forEach((button, index) => button.setAttribute('aria-pressed', String(tabs[index][0] === value)));
        draw();
    }, 'Source');
    section.append(tabBar, body);
    draw();
    return section;
}
