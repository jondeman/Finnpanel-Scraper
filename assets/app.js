// Wiring: tabs, controls, routing between the list pages and title pages, and the title search.
// Routes: #/online (default), #/totaltv, #/title/<title key>.

const SOURCE_SETTINGS = {
    online: {
        subtitle: 'Yle Areenan, MTV Katsomon ja Ruudun katsotuimmat ohjelmat 2.9.24 alkaen',
        allLabel: 'All services', groupLabel: names => names.online,
        views: [['list', 'List'], ['compare', 'Compare demos']],
    },
    totaltv: {
        subtitle: 'Ylen, MTV:n ja Sanoman katsotuimmat ohjelmat, tv + verkko (Total TV) 24.11.26 alkaen',
        allLabel: 'All broadcasters', groupLabel: names => names.group,
        views: [['chart', 'TV + online'], ['list', 'List'], ['compare', 'Compare demos']],
    },
};

let lastListRoute = '#/online';
let loadToken = 0;

function currentDate() {
    return document.getElementById('dateSelect').value;
}

function showPage(page) {
    document.getElementById('listsPage').hidden = page !== 'lists';
    document.getElementById('titlePage').hidden = page !== 'title';
    document.querySelectorAll('.tabs a').forEach(tab => {
        tab.setAttribute('aria-current', String(page === 'lists' && tab.dataset.source === state.source));
    });
}

function route() {
    const hash = location.hash || '#/online';
    const titleMatch = hash.match(/^#\/title\/(.+)$/);
    tooltip.hidden = true;
    if (titleMatch) {
        showPage('title');
        document.querySelectorAll('.tabs a').forEach(tab => tab.setAttribute('aria-current', 'false'));
        renderTitlePage(decodeURIComponent(titleMatch[1]));
        return;
    }
    document.title = 'Finnpanel Dashboard';
    const source = hash.startsWith('#/totaltv') ? 'totaltv' : 'online';
    lastListRoute = `#/${source}`;
    const changed = source !== state.source || !state.loaded;
    state.source = source;
    showPage('lists');
    if (changed) {
        refreshControls();
        fetchData();
    } else {
        render();
    }
}

function refreshControls() {
    const settings = SOURCE_SETTINGS[state.source];
    document.getElementById('subtitle').textContent = settings.subtitle;
    document.getElementById('dataRange').hidden = state.source !== 'online';

    const service = document.getElementById('service');
    const previous = service.value || 'all';
    service.innerHTML = '';
    service.add(new Option(settings.allLabel, 'all'));
    Object.entries(GROUPS).forEach(([key, names]) => service.add(new Option(settings.groupLabel(names), key)));
    service.value = previous;

    const views = document.getElementById('viewSwitch');
    views.innerHTML = '';
    settings.views.forEach(([view, label]) => {
        const button = el('button', null, label);
        button.type = 'button';
        button.dataset.view = view;
        button.setAttribute('aria-pressed', String(state.view[state.source] === view));
        button.addEventListener('click', () => setView(view));
        views.appendChild(button);
    });
}

function setView(view) {
    state.view[state.source] = view;
    document.querySelectorAll('#viewSwitch button').forEach(button =>
        button.setAttribute('aria-pressed', String(button.dataset.view === view)));
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

async function fetchData() {
    const token = ++loadToken;
    const loading = document.getElementById('loading');
    loading.style.display = 'block';
    document.getElementById('listsBody').classList.add('refetching');
    setMessage('');
    const date = currentDate();
    try {
        const manifest = await loadManifest();
        const timeframe = document.getElementById('timeframe');
        if (state.source === 'online') {
            const period = document.getElementById('dataRange').value;
            const {date: shown, rows} = await loadListDay('online', date, row => row.Period === period);
            if (token !== loadToken) return;
            state.rows = rows;
            if (!shown) {
                timeframe.textContent = '';
                setMessage(`Online lists are collected from ${finnishDate(manifest.online.first)}.`,
                    {label: `Show ${finnishDate(manifest.online.first)}`, run: () => showDate(manifest.online.first)});
            } else {
                if (shown !== date) setMessage(`No online list for ${finnishDate(date)}; showing ${finnishDate(shown)}, the latest before it.`);
                const window = rowWindow(rows[0]);
                timeframe.textContent = `Window: ${windowText(window.start, window.end)} (list of ${finnishDate(shown)})`;
            }
        } else {
            const {date: shown, rows: dayRows} = await loadListDay('totaltv', date);
            if (token !== loadToken) return;
            if (!shown) {
                state.rows = [];
                timeframe.textContent = '';
                setMessage(`Total TV lists are collected from ${finnishDate(manifest.totaltv.first)}. Finnpanel publishes no archive, so earlier days do not exist.`,
                    {label: `Show ${finnishDate(manifest.totaltv.first)}`, run: () => showDate(manifest.totaltv.first)});
            } else {
                if (shown !== date) {
                    setMessage(`No Total TV list for ${finnishDate(date)}; showing ${finnishDate(shown)}, the latest before it.`);
                }
                const rows = dayRows.map(row => ({...row}));
                const [online, broadcasts] = await Promise.all([
                    loadListDay('online', shown, row => row.Period === '14D').catch(() => ({rows: []})),
                    loadBroadcasts().catch(() => []),
                ]);
                if (token !== loadToken) return;
                // Only the online list of the same day covers the same 14 days
                enrichTotalTv(rows, online.date === shown ? online.rows : [], broadcasts);
                state.rows = rows;
                timeframe.textContent = `Window: ${windowText(rows[0].PeriodStart, rows[0].PeriodEnd)} (list of ${finnishDate(shown)})`;
            }
        }
        state.loaded = true;
        updateDemoOptions();
        state.comparison = buildComparison(state.rows);
        render();
    } catch (error) {
        console.error(error);
        setMessage(`Could not load the data: ${error.message}`);
    } finally {
        if (token === loadToken) {
            loading.style.display = 'none';
            document.getElementById('listsBody').classList.remove('refetching');
        }
    }
}

function showDate(date) {
    document.getElementById('dateSelect').value = date;
    fetchData();
}

function render() {
    const view = state.view[state.source];
    document.getElementById('dataTable').hidden = view !== 'list';
    document.getElementById('compareView').hidden = view !== 'compare';
    document.getElementById('tvView').hidden = view !== 'chart';
    document.getElementById('demo').hidden = view === 'compare';
    const onlyAllViewers = state.rows.length > 0 && !state.hasDemoData;
    document.getElementById('demoNote').style.display = view !== 'compare' && onlyAllViewers ? 'block' : 'none';
    if (view === 'list') updateTable();
    else if (view === 'compare') updateCompare();
    else updateTvChart();
}

function downloadXLSX() {
    if (state.rows.length === 0) {
        alert('No data to download. Please wait for data to load.');
        return;
    }
    const date = currentDate();
    if (state.source === 'online') {
        const period = document.getElementById('dataRange').value;
        const columns = ['Date', 'Period', 'Demo', 'Rank', 'Service', 'ServiceRank', 'Program', 'Episode', 'Duration', 'Viewers', 'PeriodStart', 'PeriodEnd', 'Population'];
        const numeric = new Set(['Rank', 'ServiceRank', 'Viewers', 'Population']);
        const rows = state.rows.map(row => Object.fromEntries(columns.map(column =>
            [column, numeric.has(column) && row[column] !== '' ? Number(row[column]) : row[column]])));
        downloadSheets(`${period}_Finnpanel_data_${state.rows[0].Date}.xlsx`, [{name: 'Finnpanel Data', rows}]);
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
    downloadSheets(`TotalTV_Finnpanel_data_${rows[0].Date}.xlsx`, [{name: 'Total TV', rows}]);
}

// ---------------------------------------------------------------- title search

let searchIndex = null;

async function prepareSearch() {
    if (searchIndex) return searchIndex;
    const input = document.getElementById('titleSearch');
    input.placeholder = 'Loading titles…';
    const history = await loadHistory();
    // Most-listed titles first, so the suggestions lead with what people look for
    searchIndex = [...history.titles.values()]
        .map(title => ({key: title.key, name: title.name, weight: title.totaltv.length * 4 + title.online.length + title.tv.length * 2}))
        .sort((a, b) => b.weight - a.weight);
    const options = document.getElementById('titleOptions');
    options.innerHTML = '';
    const seen = new Set();
    searchIndex.forEach(item => {
        if (seen.has(item.name)) return;
        seen.add(item.name);
        options.appendChild(new Option(item.name));
    });
    input.placeholder = 'Find a title…';
    return searchIndex;
}

function findTitle(text) {
    if (!searchIndex) return null;
    const query = text.toLocaleLowerCase('fi').trim();
    if (!query) return null;
    return searchIndex.find(item => item.name.toLocaleLowerCase('fi') === query)
        || searchIndex.find(item => item.key.startsWith(query))
        || searchIndex.find(item => item.key.includes(query));
}

function goToSearch(text) {
    const found = findTitle(text);
    if (found) {
        document.getElementById('titleSearch').value = '';
        location.hash = `#/title/${encodeURIComponent(found.key)}`;
    }
}

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
        updateTvChart();
    });
});
document.getElementById('includeOutside').addEventListener('change', updateCompare);
document.getElementById('showAll').addEventListener('click', () => {
    state.showAllRows = !state.showAllRows;
    updateCompare();
});
document.getElementById('tvShowAll').addEventListener('click', () => {
    tvShowAll = !tvShowAll;
    updateTvChart();
});
document.getElementById('service').addEventListener('change', render);
document.getElementById('demo').addEventListener('change', render);
document.getElementById('dateSelect').addEventListener('change', fetchData);
document.getElementById('dataRange').addEventListener('change', fetchData);
document.getElementById('downloadBtn').addEventListener('click', downloadXLSX);

const searchInput = document.getElementById('titleSearch');
searchInput.addEventListener('focus', () => prepareSearch().catch(error => {
    searchInput.placeholder = 'Titles could not be loaded';
    console.error(error);
}));
searchInput.addEventListener('change', () => goToSearch(searchInput.value));
searchInput.addEventListener('keydown', event => {
    if (event.key === 'Enter') goToSearch(searchInput.value);
});

let resizeTimer = null;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
        if (!document.getElementById('titlePage').hidden) titleCharts.forEach(draw => draw());
    }, 150);
});
window.addEventListener('hashchange', route);

// Start on the newest list. Falls back to yesterday if the manifest is unreachable.
(async () => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    let start = isoDate(yesterday);
    try {
        const manifest = await loadManifest();
        if (manifest.online && manifest.online.last) start = manifest.online.last;
    } catch (error) {
        console.warn('No manifest, starting from yesterday', error);
    }
    document.getElementById('dateSelect').value = start;
    route();
})();
