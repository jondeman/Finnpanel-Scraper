// Title page: everything known about one programme. Total TV (TV + online, from 2026-11-24),
// the online lists (Yle Areena, MTV Katsomo, Ruutu: 3+ since 2024-08-30, every demo since
// 2026-10-07) and its linear airings (the weekly broadcast lists, collected from week 38/2026).

// Colours come from the stylesheet, so each is defined once (assets/dashboard.css)
const cssVar = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const SOURCE_COLORS = {totaltv: cssVar('--src-totaltv'), online: cssVar('--src-online'), tv: cssVar('--src-linear')};
const AGE_COLORS = {'Alle 45': cssVar('--under45'), '45+': cssVar('--over45')};
const titleState = {key: null, demo: '3+', range: 'all', window: '14D', mix: null, dataTab: 'tv', showAll: {}};
const RANGE_DAYS = {'90': 90, '365': 365, all: null};
let titleCharts = [];

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

function backLink() {
    return link(lastListRoute, '← Takaisin listaan', 'back-link');
}

async function renderTitlePage(key) {
    const page = document.getElementById('titlePage');
    if (titleState.key !== key) {
        Object.assign(titleState, {key, demo: '3+', range: 'all', window: '14D', mix: null, dataTab: 'tv', showAll: {}});
        window.scrollTo(0, 0);
    }
    if (!page.dataset.key || page.dataset.key !== key) {
        page.innerHTML = '';
        page.appendChild(el('p', 'title-loading', 'Ladataan ohjelman historiaa…'));
    }
    page.dataset.key = key;
    let history;
    try {
        history = await loadHistory();
    } catch (error) {
        page.innerHTML = '';
        page.appendChild(el('p', 'title-loading', `Historiaa ei voitu ladata: ${error.message}`));
        return;
    }
    if (titleState.key !== key) return; // another title was opened meanwhile
    const title = history.titles.get(key);
    page.innerHTML = '';
    page.appendChild(backLink());
    if (!title) {
        page.appendChild(el('h2', 'title-name', 'Ohjelmaa ei löytynyt'));
        page.appendChild(el('p', null, 'Millään listalla ei ole tämännimistä ohjelmaa. Kokeile hakua yläkulmassa.'));
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
    const windowLabel = titleState.window === '14D' ? '14 päivää' : '90 päivää';

    // ---- header: what the programme is and where the figures come from
    const services = [...new Set([...title.totaltv.map(row => row.Group), ...title.online.map(row => row.Service)])];
    const channels = [...new Set(title.tv.map(row => row.Channel))];
    const header = el('div', 'title-header');
    header.appendChild(el('h2', 'title-name', title.name));
    const facts = [];
    if (services.length) facts.push(services.join(', '));
    if (channels.length) facts.push(`tv:ssä ${channels.join(', ')}`);
    header.appendChild(el('p', 'title-facts', facts.join(' · ')));
    const onlineDates = [...new Set(title.online.filter(row => row.Period === '14D').map(row => row.Date))].sort();
    const totaltvDates = [...new Set(title.totaltv.map(row => row.Date))].sort();
    const weeks = history.manifest.tv.weeks || [];
    const coverage = el('ul', 'coverage');
    coverage.append(
        coverageItem('totaltv', 'Total TV', totaltvDates.length
            ? `${totaltvDates.length} päivän listalla` : 'ei vielä listalla',
        history.listDates.totaltv.length ? `kerätty ${finnishDate(history.listDates.totaltv[0])} alkaen` : ''),
        coverageItem('online', 'Verkkopalvelut', onlineDates.length
            ? `${onlineDates.length} päivän listalla, ${finnishDate(onlineDates[0])}–${finnishDate(onlineDates[onlineDates.length - 1])}` : 'ei listalla',
        history.listDates.online.length ? `kerätty ${finnishDate(history.listDates.online[0])} alkaen` : ''),
        coverageItem('tv', 'Tv-lähetykset', airings.length
            ? `${airings.length} lähetystä viikkolistoilla` : 'ei viikkolistoilla',
        weeks.length ? `kerätty viikosta ${Number(weeks[0].slice(-2))}/${weeks[0].slice(0, 4)} alkaen` : ''),
    );
    header.appendChild(coverage);
    page.appendChild(header);

    // ---- controls that scope everything below
    const redraw = () => drawTitleAgain(title, history);
    const controls = el('div', 'title-controls');
    const demosWithData = new Set([...title.totaltv, ...title.online].map(row => row.Demo));
    const group = (label, control) => {
        const wrap = el('div', 'filter');
        wrap.append(el('span', 'filter-label', label), control);
        return wrap;
    };
    controls.append(
        group('Kohderyhmä', segmented(DEMO_ORDER.map(option => [option, option === '3+' ? 'Kaikki 3+' : DEMO_SHORT[option],
            demosWithData.has(option) ? null : 'Ohjelma ei ole tämän kohderyhmän listoilla']), demo, value => { titleState.demo = value; redraw(); }, 'Kohderyhmä')),
        group('Aikaväli', segmented([['90', '3 kk'], ['365', '1 vuosi'], ['all', 'Koko historia']], titleState.range, value => { titleState.range = value; redraw(); }, 'Aikaväli')),
        group('Verkkolistan ajanjakso', segmented([['14D', '14 päivää'], ['90D', '90 päivää']], titleState.window, value => { titleState.window = value; redraw(); }, 'Verkkolistan ajanjakso')),
    );
    page.appendChild(controls);

    // ---- headline figures
    page.appendChild(renderHeadline(title, history, totaltvByDemo, onlineByDemo, airings));

    // ---- shortcuts to the sections of a long page
    const sections = [];
    const jump = el('nav', 'jump-links');
    jump.setAttribute('aria-label', 'Sivun osiot');
    jump.appendChild(el('span', 'filter-label', 'Siirry'));
    page.appendChild(jump);
    const addSection = (section, label) => {
        sections.push([section, label]);
        page.appendChild(section);
    };

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
    const viewersCard = card('Katsojamäärän kehitys',
        `${DEMO_LABELS[demo]}. Jokainen piste on listan katsotuin jakso ajanjaksolla, piirrettynä ajanjakson viimeiselle päivälle. Viiva katkeaa, kun ohjelma ei ollut kärki-20:ssä.${demo === '3+' ? ' Oranssit pisteet ovat yksittäisiä tv-lähetyksiä (7 vuorokauden luku, kun se on julkaistu).' : ' Tv-lähetykset mitataan vain kaikista katsojista (3+).'}`);
    const chart = el('div', 'chart-box');
    viewersCard.appendChild(chart);
    addSection(viewersCard, 'Kehitys');
    const series = [
        {id: 'totaltv', label: 'Total TV: tv + verkko, 14 päivää', color: SOURCE_COLORS.totaltv, kind: 'line', gapDays: 2,
            points: totaltvByDemo.map(row => ({t: parseIso(row.PeriodEnd).getTime(), y: Number(row.Viewers),
                lines: [`${row.Rank}. yhteisellä listalla, ${row.GroupRank}. ${genitive(row.Group)} listalla`, `ajanjakso ${windowText(row.PeriodStart, row.PeriodEnd)}`, row.Episode].filter(Boolean)}))},
        {id: 'online', label: `Verkkopalvelut, ${windowLabel}`, color: SOURCE_COLORS.online, kind: 'line', gapDays: 2,
            points: onlineByDemo.map(row => {
                const window = rowWindow(row);
                const episode = ruutuEpisode(row.Program);
                return {t: parseIso(window.end).getTime(), y: Number(row.Viewers),
                    lines: [`${row.ServiceRank}. ${genitive(row.Service)} listalla`, `ajanjakso ${windowText(window.start, window.end)}`,
                        row.Episode || (episode ? episodeLabel(episode) : '')].filter(Boolean)};
            })},
    ];
    if (demo === '3+') {
        series.push({id: 'tv', label: 'Tv-lähetykset', color: SOURCE_COLORS.tv, kind: 'dots',
            points: airings.map(row => ({t: parseIso(row.Date).getTime(), y: airingValue(row),
                lines: [`${row.Channel} ${WEEKDAY_NAMES[row.Weekday] || row.Weekday} klo ${row.Time}`, `tavoittavuus ${fullNumber(row.Reach)}`,
                    row.Viewers7d ? `7 vrk:n luku (samana päivänä ${fullNumber(row.ViewersSameDay)})` : 'saman päivän luku; 7 vrk:n lukua ei vielä julkaistu']}))});
    }
    const drawCharts = [() => renderTimeChart(chart, series, {from, to, height: 300,
        describe: `${title.name}: katsojamäärän kehitys`, emptyText: 'Tälle kohderyhmälle ei ole lukuja tällä aikavälillä.'})];

    // ---- chart 2: under 45 vs 45+
    const mixCard = card('Ikäryhmät: alle 45 ja 45+',
        'Alle 45-vuotiaat ja 45 vuotta täyttäneet ovat yhdessä kaikki katsojat. Katko tarkoittaa, ettei ohjelma ollut ryhmän kärki-20:ssä. Kohderyhmät kerätään verkkopalveluista 7.10.2026 ja Total TV:stä 24.11.2026 alkaen.');
    const mixSources = [['totaltv', 'Total TV'], ['online', `Verkkopalvelut, ${windowLabel}`]];
    if (!titleState.mix) {
        // Open on whichever source has more demo history for this title
        const demoDays = rows => new Set(rows.filter(row => row.Demo === 'Alle 45' || row.Demo === '45+').map(row => row.Date)).size;
        titleState.mix = demoDays(title.totaltv) >= demoDays(onlineRows) ? 'totaltv' : 'online';
    }
    mixCard.querySelector('.card-head').appendChild(segmented(mixSources, titleState.mix, value => { titleState.mix = value; redraw(); }, 'Lähde'));
    const mixChart = el('div', 'chart-box');
    mixCard.appendChild(mixChart);
    addSection(mixCard, 'Ikäryhmät');
    const mixRows = titleState.mix === 'totaltv' ? title.totaltv : onlineRows;
    const mixSeries = [['Alle 45', 'Alle 45-vuotiaat'], ['45+', '45 vuotta täyttäneet']].map(([demoName, label]) => ({
        id: demoName, label, color: AGE_COLORS[demoName], kind: 'line', gapDays: 2,
        points: byDateBest(mixRows.filter(row => row.Demo === demoName)).map(row => ({
            t: parseIso(rowWindow(row).end).getTime(), y: Number(row.Viewers),
            lines: [`${row.GroupRank || row.ServiceRank}. ${genitive(row.Service)} listalla`],
        })),
    }));
    // Demo lists are recent, so this chart starts where its own figures start rather than leaving
    // most of its width empty
    const mixFirst = Math.min(...mixSeries.flatMap(item => item.points.map(point => point.t)).filter(t => t >= from));
    const mixFrom = Number.isFinite(mixFirst) ? Math.max(from, Math.min(mixFirst - 3 * DAY_MS, to - 14 * DAY_MS)) : from;
    drawCharts.push(() => renderTimeChart(mixChart, mixSeries, {from: mixFrom, to, height: 240,
        describe: `${title.name}: alle 45-vuotiaat ja 45 vuotta täyttäneet`, emptyText: 'Ohjelma ei ole ollut ikäryhmien listoilla tällä aikavälillä.'}));

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
        breakdown.appendChild(renderDetail(latestOnline.show, {heading: `Verkkopalvelut, ${windowText(window.start, window.end)}`}));
    }
    if (breakdown.children.length) {
        const breakdownCard = card('Kohderyhmät viimeisimmällä listalla', 'Tuorein lista, jolla ohjelma on. Sija on kaikkien kolmen yhteisellä listalla; tumma sija = kärkikymmenikkö.');
        breakdownCard.appendChild(breakdown);
        addSection(breakdownCard, 'Kohderyhmät');
    }

    // ---- linear airings
    if (airings.length) {
        const airingsCard = card('Tv-lähetykset', 'Jokainen lähetys Finnpanelin viikkolistoilla: kanavien top 20, kaikkien kanavien top 100 ja top 50. Kaikki katsojat (3+). Finnpanel pitää näkyvillä vain 10 viikkoa; työkalu säilyttää kaiken kerätyn.');
        airingsCard.appendChild(table(
            ['Päivä', 'Klo', 'Kanava', 'Nimi listalla', 'Tavoittavuus', 'Katsojia, sama päivä', 'Katsojia, 7 vrk', 'Sija kanavalla', 'Top 100'],
            [...airings].reverse().map(row => [
                `${WEEKDAY_NAMES[row.Weekday] || row.Weekday} ${finnishDate(row.Date)}`, row.Time, row.Channel, row.Program,
                num(row.Reach), num(row.ViewersSameDay), num(row.Viewers7d), row.ChannelRank ? `${row.ChannelRank}.` : '', row.Top100Rank ? `${row.Top100Rank}.` : '',
            ]), 'airings', 20));
        addSection(airingsCard, 'Tv-lähetykset');
    }

    // ---- episodes
    const episodes = episodeRows(title);
    if (episodes.length) {
        const episodesCard = card('Katsotuimmat jaksot', 'Listoilla nimetyt jaksot ja kunkin paras luku kaikista katsojista (3+). Verkkopalveluiden jaksonimet kerätään 24.11.2026 alkaen; Ruudun nimissä kausi ja jakso ovat mukana.');
        episodesCard.appendChild(table(['Jakso', 'Lähde', 'Paras', 'Listoilla'],
            episodes.map(item => [item.episode, item.source, fullNumber(item.best), item.first === item.last ? finnishDate(item.first) : `${finnishDate(item.first)}–${finnishDate(item.last)}`]),
            'episodes', 15));
        addSection(episodesCard, 'Jaksot');
    }

    // ---- all rows
    addSection(renderDataCard(title), 'Kaikki luvut');
    sections.forEach(([section, label]) => {
        const button = el('button', 'jump', label);
        button.type = 'button';
        button.addEventListener('click', () => section.scrollIntoView({behavior: 'smooth', block: 'start'}));
        jump.appendChild(button);
    });
    drawCharts.forEach(draw => draw());
    titleCharts = drawCharts;
}

function drawTitleAgain(title, history) {
    const page = document.getElementById('titlePage');
    const scroll = window.scrollY;
    page.innerHTML = '';
    page.appendChild(backLink());
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

function renderHeadline(title, history, totaltvByDemo, onlineByDemo, airings) {
    const wrap = el('div', 'headline');
    const demo = titleState.demo;
    const latestTotalDate = history.listDates.totaltv.slice(-1)[0];
    const latestOnlineDate = history.listDates.online.slice(-1)[0];
    const windowDays = titleState.window === '14D' ? '14' : '90';

    const hero = el('div', 'hero-block');
    const lastTotal = totaltvByDemo[totaltvByDemo.length - 1];
    const lastOnline = onlineByDemo[onlineByDemo.length - 1];
    if (lastTotal) {
        hero.append(el('span', 'hero-num', fullNumber(lastTotal.Viewers)),
            el('span', 'hero-label', `katsojaa tv:ssä ja verkossa, ${windowText(lastTotal.PeriodStart, lastTotal.PeriodEnd)} (${DEMO_SHORT[demo]})`),
            el('span', 'hero-sub', `${lastTotal.Rank}. Ylen, MTV:n ja Sanoman yhteisellä listalla · ${lastTotal.GroupRank}. ${genitive(lastTotal.Group)} listalla${lastTotal.Date !== latestTotalDate ? ` · viimeksi listalla ${finnishDate(lastTotal.Date)}` : ''}`));
    } else if (lastOnline) {
        const window = rowWindow(lastOnline);
        hero.append(el('span', 'hero-num', fullNumber(lastOnline.Viewers)),
            el('span', 'hero-label', `katsojaa verkossa, ${windowText(window.start, window.end)} (${DEMO_SHORT[demo]})`),
            el('span', 'hero-sub', `${lastOnline.ServiceRank}. ${genitive(lastOnline.Service)} listalla${lastOnline.Date !== latestOnlineDate ? ` · viimeksi listalla ${finnishDate(lastOnline.Date)}` : ''} · ei Total TV -listalla`));
    } else if (airings.length) {
        const best = airings.reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
        hero.append(el('span', 'hero-num', fullNumber(airingValue(best))),
            el('span', 'hero-label', 'katsojaa parhaassa tv-lähetyksessä (3+)'),
            el('span', 'hero-sub', `${best.Channel} ${WEEKDAY_NAMES[best.Weekday]} ${finnishDate(best.Date)} klo ${best.Time} · ei verkon eikä Total TV:n kärki-20:ssä`));
    } else {
        hero.append(el('span', 'hero-label', `Ei kohderyhmän ${DEMO_SHORT[demo]} listoilla.`));
    }
    wrap.appendChild(hero);

    const tiles = el('div', 'stat-tiles');
    if (lastOnline && lastTotal) {
        const sameWindow = byDateBest(title.online.filter(row => row.Period === '14D' && row.Demo === demo))
            .find(row => row.Date === lastTotal.Date);
        tiles.appendChild(statTile('Verkossa samana aikana',
            sameWindow ? fullNumber(sameWindow.Viewers) : '–',
            sameWindow ? `${percent(Math.min(sameWindow.Viewers / lastTotal.Viewers, 1))} Total TV:stä · ${sameWindow.ServiceRank}. ${genitive(sameWindow.Service)} listalla` : 'ei verkon kärki-20:ssä samana päivänä'));
    } else if (lastOnline) {
        tiles.appendChild(statTile(`Verkossa, ${windowDays} päivää`, fullNumber(lastOnline.Viewers),
            `${lastOnline.ServiceRank}. ${genitive(lastOnline.Service)} listalla ${finnishDate(lastOnline.Date)}`));
    }
    if (onlineByDemo.length) {
        const peak = onlineByDemo.reduce((a, b) => Number(b.Viewers) > Number(a.Viewers) ? b : a);
        const window = rowWindow(peak);
        tiles.appendChild(statTile(`Verkon huippu, ${windowDays} päivää`, fullNumber(peak.Viewers),
            `ajanjakso ${windowText(window.start, window.end)}`));
    }
    if (airings.length) {
        const lastWeek = airings[airings.length - 1].Week;
        const best = airings.filter(row => row.Week === lastWeek).reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
        tiles.appendChild(statTile(`Paras tv-lähetys, viikko ${lastWeek}`, fullNumber(airingValue(best)),
            `${best.Channel} ${WEEKDAY_NAMES[best.Weekday]} ${finnishDate(best.Date, false)} klo ${best.Time} · tavoittavuus ${compact(Number(best.Reach))}`));
        if (lastTotal) {
            const inWindow = airings.filter(row => row.Date >= lastTotal.PeriodStart && row.Date <= lastTotal.PeriodEnd);
            if (inWindow.length) {
                const top = inWindow.reduce((a, b) => airingValue(b) > airingValue(a) ? b : a);
                const ratio = Number(lastTotal.Viewers) / airingValue(top);
                tiles.appendChild(statTile('Total TV vs. paras lähetys', `×${decimal(ratio)}`,
                    `Total TV ${compact(Number(lastTotal.Viewers))}, paras lähetys ${compact(airingValue(top))} (${top.Channel}) samoina 14 päivänä`));
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

// The demo card for the latest list the title is in, built the same way as Kohderyhmät
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
        cells.forEach(cell => tr.appendChild(el('td', /^[\d×≈≤–]/.test(String(cell)) && !/^\d+\.\d+\./.test(String(cell)) ? 'num' : null, cell)));
        body.appendChild(tr);
    });
    tableEl.append(head, body);
    wrap.appendChild(tableEl);
    if (rows.length > limit) {
        const more = el('button', 'show-all', showAll ? `Näytä ${limit} ensimmäistä` : `Näytä kaikki ${rows.length} riviä`);
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
    const section = card('Kaikki luvut', 'Ohjelman jokainen rivi kaikilta listoilta, uusin ensin. Excel-tiedostossa kukin lähde on omalla välilehdellään.');
    const download = el('button', 'secondary-btn', 'Lataa Excel');
    download.type = 'button';
    download.addEventListener('click', () => downloadSheets(`${title.name.replace(/[\\/:*?"<>|]/g, '')}_Finnpanel.xlsx`, [
        {name: 'Total TV', rows: title.totaltv.map(({Service, Period, ...row}) => row)},
        {name: 'Verkkopalvelut', rows: title.online},
        {name: 'Tv-lähetykset', rows: title.tv},
    ]));
    section.querySelector('.card-head').appendChild(download);
    const tabs = [
        ['tv', `Tv-lähetykset (${title.tv.length})`],
        ['totaltv', `Total TV (${title.totaltv.length})`],
        ['online', `Verkkopalvelut (${title.online.length})`],
    ];
    const body = el('div');
    const draw = () => {
        body.innerHTML = '';
        const tab = titleState.dataTab;
        if (tab === 'tv') {
            body.appendChild(table(['Viikko', 'Päivä', 'Klo', 'Kanava', 'Nimi listalla', 'Tavoittavuus', 'Sama päivä', '7 vrk'],
                [...title.tv].sort((a, b) => b.Date.localeCompare(a.Date)).map(row => [`${row.Week}/${row.Year}`, finnishDate(row.Date), row.Time, row.Channel, row.Program, num(row.Reach), num(row.ViewersSameDay), num(row.Viewers7d)]),
                'data-tv', 30));
        } else if (tab === 'totaltv') {
            body.appendChild(table(['Lista', 'Ajanjakso', 'Kohderyhmä', 'Sija', 'Mediatalon sija', 'Ohjelma', 'Jakso', 'Katsojia'],
                [...title.totaltv].sort((a, b) => b.Date.localeCompare(a.Date) || demoSortKey(a.Demo) - demoSortKey(b.Demo)).map(row => [finnishDate(row.Date), windowText(row.PeriodStart, row.PeriodEnd), row.Demo, `${row.Rank}.`, `${row.GroupRank}. ${row.Group}`, row.Program, row.Episode, num(row.Viewers)]),
                'data-totaltv', 30));
        } else {
            body.appendChild(table(['Lista', 'Ajanjakso', 'Kohderyhmä', 'Sija', 'Palvelun sija', 'Ohjelma', 'Jakso', 'Katsojia'],
                [...title.online].sort((a, b) => b.Date.localeCompare(a.Date) || a.Period.localeCompare(b.Period) || demoSortKey(a.Demo) - demoSortKey(b.Demo)).map(row => {
                    const window = rowWindow(row);
                    return [finnishDate(row.Date), `${row.Period === '14D' ? '14' : '90'} pv, ${windowText(window.start, window.end)}`, row.Demo, `${row.Rank}.`, `${row.ServiceRank}. ${row.Service}`, row.Program, row.Episode, num(row.Viewers)];
                }),
                'data-online', 30));
        }
    };
    const tabBar = segmented(tabs, titleState.dataTab, value => {
        titleState.dataTab = value;
        tabBar.querySelectorAll('button').forEach((button, index) => button.setAttribute('aria-pressed', String(tabs[index][0] === value)));
        draw();
    }, 'Lähde');
    section.append(tabBar, body);
    draw();
    return section;
}
