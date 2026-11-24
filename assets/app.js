// Wiring: tabs, filters, routing between the list pages and title pages, and the title search.
// Routes: #/totaltv (default), #/online, #/title/<title key>. A list route carries its view and
// filters (#/online?view=compare&demo=45%2B&service=mtv&period=90D&date=2026-11-01), so a link
// opens the same view and the back button returns to it. No date means the newest list.

const SOURCE_SETTINGS = {
    totaltv: {
        subtitle: 'Ylen, MTV:n ja Nelosen katsotuimmat ohjelmat tv:ssä ja verkossa yhteensä',
        serviceLabel: 'Kanava', names: key => GROUPS[key].group,
    },
    online: {
        subtitle: 'Yle Areenan, MTV Katsomon ja Ruudun katsotuimmat ohjelmat',
        serviceLabel: 'Palvelu', names: key => GROUPS[key].online,
    },
};
const VIEW_HELP = {
    top: {
        totaltv: 'Katsotuimmat ohjelmat tv:ssä ja verkossa yhteensä. Klikkaa ohjelmaa nähdäksesi sen koko historian.',
        online: 'Katsotuimmat ohjelmat Yle Areenassa, MTV Katsomossa ja Ruudussa. Klikkaa ohjelmaa nähdäksesi sen koko historian.',
    },
    compare: {
        totaltv: 'Ohjelmien yleisö jaettuna alle 45-vuotiaisiin ja 45 vuotta täyttäneisiin. Klikkaa riviä nähdäksesi tarkemmin.',
        online: 'Ohjelmien yleisö jaettuna alle 45-vuotiaisiin ja 45 vuotta täyttäneisiin. Klikkaa riviä nähdäksesi tarkemmin.',
    },
    table: {
        totaltv: 'Listan kaikki luvut taulukkona. Klikkaa riviä nähdäksesi ohjelman koko historian.',
        online: 'Listan kaikki luvut taulukkona. Klikkaa riviä nähdäksesi ohjelman koko historian.',
    },
};
const VIEWS = ['top', 'compare', 'table'];

let lastListRoute = '#/totaltv';
let loadToken = 0;
let loadedKey = '';

// ---------------------------------------------------------------- routing and the URL

function parseHash() {
    const [path, query] = (location.hash || '#/totaltv').slice(1).split('?');
    return {path, params: new URLSearchParams(query || '')};
}

function listHash(source = state.source) {
    const params = new URLSearchParams();
    if (state.view !== 'top') params.set('view', state.view);
    if (state.filters.demo !== '3+') params.set('demo', state.filters.demo);
    if (state.filters.service !== 'all') params.set('service', state.filters.service);
    if (source === 'online' && state.filters.period !== '14D') params.set('period', state.filters.period);
    if (state.filters.date) params.set('date', state.filters.date);
    const query = params.toString();
    return `#/${source}${query ? `?${query}` : ''}`;
}

// Records the current view in the address without adding a history entry for every click,
// and keeps the other tab's link on the same filters
function syncUrl() {
    const hash = listHash();
    if (location.hash !== hash) history.replaceState(null, '', hash);
    lastListRoute = hash;
    document.querySelectorAll('.tabs a').forEach(tab => { tab.href = listHash(tab.dataset.source); });
}

function showPage(page) {
    document.getElementById('listsPage').hidden = page !== 'lists';
    document.getElementById('titlePage').hidden = page !== 'title';
    document.querySelectorAll('.tabs a').forEach(tab => {
        tab.setAttribute('aria-current', String(page === 'lists' && tab.dataset.source === state.source));
    });
}

function route() {
    const {path, params} = parseHash();
    tooltip.hidden = true;
    closeSearch();
    const titleMatch = path.match(/^\/title\/(.+)$/);
    if (titleMatch) {
        showPage('title');
        renderTitlePage(decodeURIComponent(titleMatch[1]));
        return;
    }
    document.title = 'Finnpanel Dashboard';
    state.source = path.startsWith('/online') ? 'online' : 'totaltv';
    const view = params.get('view');
    state.view = VIEWS.includes(view) ? view : 'top';
    const demo = params.get('demo');
    state.filters.demo = DEMO_ORDER.includes(demo) ? demo : '3+';
    const service = params.get('service');
    state.filters.service = service in GROUPS ? service : 'all';
    state.filters.period = state.source === 'online' && params.get('period') === '90D' ? '90D' : '14D';
    const date = params.get('date');
    state.filters.date = /^\d{4}-\d{2}-\d{2}$/.test(date || '') ? date : '';
    showPage('lists');
    refreshControls();
    syncUrl();
    const key = `${state.source}|${state.filters.period}|${state.filters.date}`;
    if (key !== loadedKey) {
        fetchData();
    } else {
        render();
    }
}

// ---------------------------------------------------------------- controls

function fillSegmented(container, options, value, onChange) {
    container.innerHTML = '';
    options.forEach(({value: optionValue, label, disabled}) => {
        const button = el('button', null, label);
        button.type = 'button';
        button.setAttribute('aria-pressed', String(optionValue === value));
        if (disabled) {
            button.disabled = true;
            button.title = disabled;
        }
        button.addEventListener('click', () => onChange(optionValue));
        container.appendChild(button);
    });
}

function refreshControls() {
    const settings = SOURCE_SETTINGS[state.source];
    document.getElementById('subtitle').textContent = settings.subtitle;
    document.getElementById('serviceLabel').textContent = settings.serviceLabel;

    document.querySelectorAll('#viewSwitch button').forEach(button =>
        button.setAttribute('aria-pressed', String(button.dataset.view === state.view)));
    document.getElementById('viewHelp').textContent = VIEW_HELP[state.view][state.source];

    fillSegmented(document.getElementById('periodSwitch'), [
        {value: '14D', label: '14 päivää'},
        {value: '90D', label: '90 päivää', disabled: state.source === 'totaltv' ? 'Total TV:stä Finnpanel julkaisee vain 14 päivän listan' : null},
    ], state.filters.period, value => { state.filters.period = value; syncUrl(); fetchData(); });

    fillSegmented(document.getElementById('serviceSwitch'), [
        {value: 'all', label: 'Kaikki'},
        ...Object.keys(GROUPS).map(key => ({value: key, label: settings.names(key)})),
    ], state.filters.service, value => { state.filters.service = value; refreshControls(); syncUrl(); render(); });

    const comparing = state.view === 'compare';
    // On phones the filters fold behind one button that says what is chosen
    const summary = [state.shownDate ? finnishDate(state.shownDate) : 'uusin', state.filters.period === '14D' ? '14 päivää' : '90 päivää',
        state.filters.service === 'all' ? 'kaikki' : settings.names(state.filters.service),
        comparing ? 'kaikki kohderyhmät' : (state.filters.demo === '3+' ? 'kaikki 3+' : DEMO_SHORT[state.filters.demo])];
    document.getElementById('filtersSummary').textContent = summary.join(' · ');
    fillSegmented(document.getElementById('demoSwitch'), DEMO_ORDER.map(demo => ({
        value: demo,
        label: demo === '3+' ? 'Kaikki 3+' : DEMO_SHORT[demo],
        disabled: comparing ? 'Kohderyhmät-näkymä näyttää kaikki ryhmät'
            : !state.demosAvailable.includes(demo) ? 'Tältä päivältä ei ole tämän kohderyhmän listaa (kerätään 7.10.2026 alkaen)' : null,
    })), comparing ? null : state.filters.demo, value => { state.filters.demo = value; refreshControls(); syncUrl(); render(); });
}

function setView(view) {
    state.view = view;
    refreshControls();
    syncUrl();
    render();
}

function setMessage(text, action) {
    const box = document.getElementById('listMessage');
    box.innerHTML = '';
    box.hidden = !text;
    if (!text) return;
    box.appendChild(el('span', null, text));
    if (action) {
        const button = el('button', 'inline-btn', action.label);
        button.type = 'button';
        button.addEventListener('click', action.run);
        box.appendChild(button);
    }
}

function showDate(date) {
    state.filters.date = date;
    syncUrl();
    fetchData();
}

async function stepDay(direction) {
    const manifest = await loadManifest();
    const current = state.shownDate || manifest[state.source].last;
    const next = addDays(current, direction);
    showDate(next >= manifest[state.source].last ? '' : next);
}

function updateDateControls(manifest) {
    const info = manifest[state.source];
    const shown = state.shownDate || info.last;
    const input = document.getElementById('dateSelect');
    input.value = shown || '';
    input.min = info.first || '';
    input.max = info.last || '';
    const atLatest = shown >= info.last;
    document.getElementById('nextDay').disabled = atLatest;
    document.getElementById('prevDay').disabled = !shown || shown <= info.first;
    const latest = document.getElementById('latestDay');
    latest.disabled = atLatest;
    latest.textContent = atLatest ? 'Uusin lista' : 'Siirry uusimpaan';
}

// ---------------------------------------------------------------- loading and rendering

async function fetchData() {
    const token = ++loadToken;
    const loading = document.getElementById('loading');
    loading.style.display = 'block';
    document.getElementById('listsBody').classList.add('refetching');
    setMessage('');
    try {
        const manifest = await loadManifest();
        const info = manifest[state.source];
        const date = state.filters.date || info.last;
        const timeframe = document.getElementById('timeframe');
        let shown = null;
        let rows = [];
        if (state.source === 'online') {
            ({date: shown, rows} = await loadListDay('online', date, row => row.Period === state.filters.period));
        } else {
            const day = await loadListDay('totaltv', date);
            shown = day.date;
            if (shown) {
                rows = day.rows.map(row => ({...row}));
                const [online, broadcasts] = await Promise.all([
                    loadListDay('online', shown, row => row.Period === '14D').catch(() => ({rows: []})),
                    loadBroadcasts().catch(() => []),
                ]);
                // Only the online list of the same day covers the same 14 days
                enrichTotalTv(rows, online.date === shown ? online.rows : [], broadcasts);
            }
        }
        if (token !== loadToken) return;
        state.rows = rows;
        state.shownDate = shown;
        loadedKey = `${state.source}|${state.filters.period}|${state.filters.date}`;
        const name = state.source === 'totaltv' ? 'Total TV -listoja' : 'Online-listoja';
        if (!shown) {
            timeframe.textContent = '';
            setMessage(`${name} on kerätty ${finnishDate(info.first)} alkaen${state.source === 'totaltv' ? ', eikä Finnpanel julkaise niistä arkistoa' : ''}.`,
                {label: `Näytä ${finnishDate(info.first)}`, run: () => showDate(info.first)});
        } else {
            if (shown !== date) setMessage(`Päivältä ${finnishDate(date)} ei ole listaa. Näytetään edellinen, ${finnishDate(shown)}.`);
            const window = rowWindow(rows[0]);
            timeframe.textContent = `Ajanjakso ${windowText(window.start, window.end)} · lista julkaistu ${finnishDate(shown)}${shown === info.last ? ' (uusin)' : ''}`;
        }
        state.demosAvailable = [...new Set(state.rows.map(row => row.Demo))];
        if (!state.demosAvailable.includes(state.filters.demo)) state.filters.demo = '3+';
        state.comparison = buildComparison(state.rows);
        updateDateControls(manifest);
        refreshControls();
        render();
    } catch (error) {
        console.error(error);
        setMessage(`Tietoja ei voitu ladata: ${error.message}`);
    } finally {
        if (token === loadToken) {
            loading.style.display = 'none';
            document.getElementById('listsBody').classList.remove('refetching');
        }
    }
}

function render() {
    document.getElementById('dataTable').hidden = state.view !== 'table';
    document.getElementById('compareView').hidden = state.view !== 'compare';
    document.getElementById('topView').hidden = state.view !== 'top';
    if (state.view === 'table') updateTable();
    else if (state.view === 'compare') updateCompare();
    else updateTopList();
}

function downloadXLSX() {
    if (state.rows.length === 0) {
        alert('Ei ladattavaa: odota, että lista latautuu.');
        return;
    }
    const day = state.rows[0].Date;
    if (state.source === 'online') {
        const columns = ['Date', 'Period', 'Demo', 'Rank', 'Service', 'ServiceRank', 'Program', 'Episode', 'Duration', 'Viewers', 'PeriodStart', 'PeriodEnd', 'Population'];
        const numeric = new Set(['Rank', 'ServiceRank', 'Viewers', 'Population']);
        const rows = state.rows.map(row => Object.fromEntries(columns.map(column =>
            [column, numeric.has(column) && row[column] !== '' ? Number(row[column]) : row[column]])));
        downloadSheets(`Online_${state.filters.period}_${day}.xlsx`, [{name: 'Online', rows}]);
        return;
    }
    const rows = state.rows.map(row => ({
        Date: row.Date, PeriodStart: row.PeriodStart, PeriodEnd: row.PeriodEnd, Demo: row.Demo, Rank: Number(row.Rank),
        Group: row.Group, GroupRank: Number(row.GroupRank), Program: row.Program, Episode: row.Episode, Duration: row.Duration,
        Viewers: Number(row.Viewers), Population: Number(row.Population),
        OnlineViewers: row.online ? row.online.viewers : '', OnlineRank: row.online ? row.online.rank : '',
        OnlineShare: row.onlineShare ?? '', OnlineAtMost: row.onlineCeiling ?? '',
        BestAiringViewers: row.bestAiring ? bestAiringViewers(row.bestAiring) : '',
        BestAiring: row.bestAiring ? `${row.bestAiring.Channel} ${row.bestAiring.Date} ${row.bestAiring.Time}` : '',
    }));
    downloadSheets(`TotalTV_${day}.xlsx`, [{name: 'Total TV', rows}]);
}

// ---------------------------------------------------------------- title search: a combobox

let searchIndex = null;
let searchMatches = [];
let searchActive = -1;
const searchInput = document.getElementById('titleSearch');
const searchList = document.getElementById('searchResults');

async function prepareSearch() {
    if (searchIndex) return searchIndex;
    searchInput.placeholder = 'Ladataan ohjelmia…';
    const history = await loadHistory();
    searchIndex = [...history.titles.values()].map(title => ({
        key: title.key, name: title.name, folded: title.name.toLocaleLowerCase('fi'),
        where: title.where.slice(0, 3).join(', '), lastDate: title.lastDate,
        weight: title.totaltv.length * 4 + title.online.length + title.tv.length * 2,
    }));
    searchInput.placeholder = 'Esim. Salatut elämät';
    return searchIndex;
}

// Names starting with the query first, then a word starting with it, then containing it;
// among equals, the most recently listed first
function findTitles(text) {
    const query = text.toLocaleLowerCase('fi').trim();
    if (!query || !searchIndex) return [];
    const score = item => (item.folded.startsWith(query) || item.key.startsWith(query) ? 2
        : item.folded.split(/[\s:–-]+/).some(word => word.startsWith(query)) ? 1
            : item.folded.includes(query) ? 0 : -1);
    return searchIndex
        .map(item => ({item, score: score(item)}))
        .filter(match => match.score >= 0)
        .sort((a, b) => b.score - a.score || b.item.lastDate.localeCompare(a.item.lastDate) || b.item.weight - a.item.weight)
        .slice(0, 8)
        .map(match => match.item);
}

function renderSearch() {
    searchList.innerHTML = '';
    if (!searchInput.value.trim()) {
        closeSearch();
        return;
    }
    if (!searchMatches.length) {
        searchList.appendChild(el('li', 'search-empty', searchIndex ? 'Ei osumia' : 'Ladataan…'));
    }
    searchMatches.forEach((item, index) => {
        const option = el('li', 'search-option');
        option.id = `search-${index}`;
        option.setAttribute('role', 'option');
        option.setAttribute('aria-selected', String(index === searchActive));
        option.append(el('strong', null, item.name),
            el('span', null, `${item.where} · viimeksi listalla ${finnishDate(item.lastDate)}`));
        option.addEventListener('pointerdown', event => {
            event.preventDefault();
            openSearchResult(item);
        });
        searchList.appendChild(option);
    });
    searchList.hidden = false;
    searchInput.setAttribute('aria-expanded', 'true');
    searchInput.setAttribute('aria-activedescendant', searchActive >= 0 ? `search-${searchActive}` : '');
}

function closeSearch() {
    searchList.hidden = true;
    searchInput.setAttribute('aria-expanded', 'false');
    searchActive = -1;
}

function openSearchResult(item) {
    searchInput.value = '';
    closeSearch();
    searchInput.blur();
    location.hash = `#/title/${encodeURIComponent(item.key)}`;
}

searchInput.addEventListener('focus', () => prepareSearch().then(() => {
    if (searchInput.value) {
        searchMatches = findTitles(searchInput.value);
        renderSearch();
    }
}).catch(error => {
    searchInput.placeholder = 'Ohjelmia ei voitu ladata';
    console.error(error);
}));
searchInput.addEventListener('input', () => {
    searchMatches = findTitles(searchInput.value);
    searchActive = searchMatches.length ? 0 : -1;
    renderSearch();
});
searchInput.addEventListener('keydown', event => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        if (!searchMatches.length) return;
        const step = event.key === 'ArrowDown' ? 1 : -1;
        searchActive = (searchActive + step + searchMatches.length) % searchMatches.length;
        renderSearch();
    } else if (event.key === 'Enter') {
        const item = searchMatches[Math.max(searchActive, 0)];
        if (item) openSearchResult(item);
    } else if (event.key === 'Escape') {
        closeSearch();
    }
});
searchInput.addEventListener('blur', closeSearch);

// ---------------------------------------------------------------- tooltip for data-tip marks

const tooltip = document.getElementById('tooltip');
document.addEventListener('pointerover', event => {
    const target = event.target.closest && event.target.closest('[data-tip]');
    if (!target) {
        tooltip.hidden = true;
        return;
    }
    tooltip.textContent = target.dataset.tip;
    tooltip.hidden = false;
    const rect = target.getBoundingClientRect();
    const left = Math.min(rect.left, document.documentElement.clientWidth - tooltip.offsetWidth - 8);
    tooltip.style.left = `${Math.max(8, left) + window.scrollX}px`;
    tooltip.style.top = `${rect.top + window.scrollY - tooltip.offsetHeight - 6}px`;
});
document.addEventListener('scroll', () => { tooltip.hidden = true; }, {passive: true});

// ---------------------------------------------------------------- events

document.querySelectorAll('#viewSwitch button').forEach(button => {
    button.addEventListener('click', () => setView(button.dataset.view));
});
document.querySelectorAll('#compareSort button').forEach(button => {
    button.addEventListener('click', () => {
        state.compareSort = button.dataset.sort;
        document.querySelectorAll('#compareSort button').forEach(other =>
            other.setAttribute('aria-pressed', String(other === button)));
        updateCompare();
    });
});
document.querySelectorAll('#tvSort button').forEach(button => {
    button.addEventListener('click', () => {
        tvSort = button.dataset.sort;
        document.querySelectorAll('#tvSort button').forEach(other =>
            other.setAttribute('aria-pressed', String(other === button)));
        updateTopList();
    });
});
document.getElementById('includeOutside').addEventListener('change', updateCompare);
document.getElementById('showAll').addEventListener('click', () => {
    state.showAllRows = !state.showAllRows;
    updateCompare();
});
document.getElementById('tvShowAll').addEventListener('click', () => {
    tvShowAll = !tvShowAll;
    updateTopList();
});
document.getElementById('dateSelect').addEventListener('change', async event => {
    const manifest = await loadManifest();
    const value = event.target.value;
    if (value) showDate(value >= manifest[state.source].last ? '' : value);
});
document.getElementById('prevDay').addEventListener('click', () => stepDay(-1));
document.getElementById('nextDay').addEventListener('click', () => stepDay(1));
document.getElementById('latestDay').addEventListener('click', () => showDate(''));
document.getElementById('downloadBtn').addEventListener('click', downloadXLSX);
document.getElementById('filtersToggle').addEventListener('click', event => {
    const open = document.getElementById('filters').classList.toggle('open');
    event.currentTarget.setAttribute('aria-expanded', String(open));
});

let resizeTimer = null;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
        if (!document.getElementById('titlePage').hidden) titleCharts.forEach(draw => draw());
    }, 150);
});
window.addEventListener('hashchange', route);
route();
