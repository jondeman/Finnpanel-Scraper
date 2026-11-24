// Shared data access: where the files live, parsing, and how a programme is recognised across lists.

// The scraper commits with GITHUB_TOKEN, which does not trigger a Pages deploy, so the published
// site would serve stale data. Read the files straight from main instead. When the dashboard is
// served from a local checkout, read the working tree so unpushed data can be tested.
const RAW_BASE = 'https://raw.githubusercontent.com/jondeman/Finnpanel-Scraper/main/';
const IS_LOCAL = location.protocol === 'file:' || ['localhost', '127.0.0.1'].includes(location.hostname);
const DATA_BASE = IS_LOCAL ? '' : RAW_BASE;

// Demos in the order the scrapers write them; files from before 2026-10-07 only have 3+
const DEMO_ORDER = ['3+', 'Alle 45', '45+', '25-64'];
// Finnpanel's names for its target groups (kohderyhmät), long and short
const DEMO_LABELS = {'3+': 'Kaikki 3 vuotta täyttäneet', 'Alle 45': 'Alle 45-vuotiaat', '45+': '45 vuotta täyttäneet', '25-64': '25–64-vuotiaat'};
const DEMO_SHORT = {'3+': '3+', 'Alle 45': 'Alle 45', '45+': '45+', '25-64': '25–64'};

// Broadcaster groups: TotalTV lists are per group, online lists per streaming service,
// linear broadcasts per channel. The key ties the three together.
const GROUPS = {
    yle: {group: 'Yle', online: 'Yle Areena'},
    mtv: {group: 'MTV', online: 'MTV Katsomo'},
    sanoma: {group: 'Sanoma', online: 'Ruutu'},
};
const GROUP_KEY_BY_NAME = {};
Object.entries(GROUPS).forEach(([key, names]) => {
    GROUP_KEY_BY_NAME[names.group] = key;
    GROUP_KEY_BY_NAME[names.online] = key;
});

function groupKeyOf(name) {
    return GROUP_KEY_BY_NAME[name] || '';
}

// "Ruudun listalla", "MTV:n listalla": Finnish genitives of the services and groups
const GENITIVES = {
    'Yle': 'Ylen', 'MTV': 'MTV:n', 'Sanoma': 'Sanoman',
    'Yle Areena': 'Yle Areenan', 'MTV Katsomo': 'MTV Katsomon', 'Ruutu': 'Ruudun',
};

function genitive(name) {
    return GENITIVES[name] || `${name}:n`;
}

// The programme name without what changes between episodes. Ruutu (and Sanoma's TotalTV list)
// carry the episode in the name: "Syke - Kausi 24 - Jakso 4 12.11.2026 06:00", sometimes cut off
// mid-word ("... - Kau"). Linear lists add an age rating: "Valamiehet (12)", "(S)".
function seriesName(program) {
    return String(program || '')
        .replace(/ - Kausi\b.*$/, '')
        .replace(/ - (K(a(u(s)?)?)?)?$/, '')
        .replace(/\s+\d{1,2}\.\d{1,2}\.\d{2,4}( \d{1,2}:\d{0,2})?$/, '')
        .replace(/\s*\((S|\d{1,2})\)$/, '')
        .trim();
}

// One key per programme across all lists. Weekly linear lists cut names at 40 characters and the
// others at 50, so only the first 40 characters count.
function titleKey(program) {
    return seriesName(program).toLocaleLowerCase('fi').replace(/\s+/g, ' ').slice(0, 40).trim();
}

function titleHref(program) {
    return `#/title/${encodeURIComponent(titleKey(program))}`;
}

// "Huvila & Huussi - Kausi 17 - Jakso 3 23.03.2026 06" -> {season: 17, episode: 3, published: '23.03.2026'}
function ruutuEpisode(program) {
    const found = String(program || '').match(/ - Kausi (\d+)(?: - Jakso (\d+))?(?: (\d{1,2}\.\d{1,2}\.\d{4}))?/);
    if (!found) return null;
    return {season: Number(found[1]), episode: found[2] ? Number(found[2]) : null, published: found[3] || null};
}

// "kausi 17, jakso 3, julkaistu 23.03.2026"
function episodeLabel(episode) {
    return [`kausi ${episode.season}`, episode.episode ? `jakso ${episode.episode}` : null,
        episode.published ? `julkaistu ${episode.published}` : null].filter(Boolean).join(', ');
}

// RFC 4180 CSV, as written by Python's csv module
function parseCsv(text) {
    const rows = [];
    let row = [];
    let field = '';
    let quoted = false;
    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        if (quoted) {
            if (char === '"') {
                if (text[i + 1] === '"') {
                    field += '"';
                    i++;
                } else {
                    quoted = false;
                }
            } else {
                field += char;
            }
        } else if (char === '"') {
            quoted = true;
        } else if (char === ',') {
            row.push(field);
            field = '';
        } else if (char === '\n') {
            row.push(field);
            rows.push(row);
            row = [];
            field = '';
        } else if (char !== '\r') {
            field += char;
        }
    }
    if (field !== '' || row.length) {
        row.push(field);
        rows.push(row);
    }
    const header = rows.shift() || [];
    return rows.filter(cells => cells.length > 1).map(cells => {
        const record = {};
        header.forEach((name, index) => { record[name] = cells[index] ?? ''; });
        return record;
    });
}

const fetchCache = new Map();

function fetchOnce(path, read) {
    const key = `${read}:${path}`;
    if (!fetchCache.has(key)) {
        const request = fetch(DATA_BASE + path).then(response => {
            if (!response.ok) throw new Error(`${response.status} for ${path}`);
            return response[read]();
        });
        request.catch(() => fetchCache.delete(key));
        fetchCache.set(key, request);
    }
    return fetchCache.get(key);
}

async function fetchCsv(path) {
    return parseCsv(await fetchOnce(path, 'text'));
}

function loadManifest() {
    return fetchOnce('data/manifest.json', 'json');
}

// The data files are monthly (online, Total TV) or yearly (linear TV) CSVs listed in the manifest.
// Each file is fetched and prepared once per visit.
const preparedFiles = new Map();

function loadFile(folder, file) {
    if (!preparedFiles.has(file)) {
        const request = fetchCsv(file).then(rows => prepareRows(folder, rows));
        request.catch(() => preparedFiles.delete(file));
        preparedFiles.set(file, request);
    }
    return preparedFiles.get(file);
}

function prepareRows(folder, rows) {
    if (folder === 'totaltv') {
        rows.forEach(row => {
            row.Service = row.Group;
            row.Period = '14D';
        });
    } else if (folder === 'online') {
        fillServiceRanks(rows);
    }
    return rows;
}

// Rows converted from the xlsx archive (before 2026-11-24) lack Finnpanel's rank within the
// service's own list. It is the row's position by viewers in that list; tied rows share a rank.
function fillServiceRanks(rows) {
    const lists = new Map();
    rows.forEach(row => {
        if (row.ServiceRank) return;
        const key = `${row.Date}|${row.Period}|${row.Demo}|${row.Service}`;
        if (!lists.has(key)) lists.set(key, []);
        lists.get(key).push(row);
    });
    lists.forEach(list => list.forEach(row => {
        row.ServiceRank = String(1 + list.filter(other => Number(other.Viewers) > Number(row.Viewers)).length);
    }));
}

async function loadFolder(folder) {
    const manifest = await loadManifest();
    const files = await Promise.all(manifest[folder].files.map(file => loadFile(folder, file)));
    return files.flat();
}

function fileMonth(file) {
    const found = String(file).match(/(\d{4}(?:-\d{2})?)\.csv$/);
    return found ? found[1] : '';
}

// The newest list on or before `date` among rows passing `filter`, looking back month by month.
// Returns {date: null, rows: []} when there is none.
async function loadListDay(folder, date, filter = () => true) {
    const manifest = await loadManifest();
    const files = manifest[folder].files.filter(file => fileMonth(file) <= date.slice(0, 7)).reverse();
    for (const file of files) {
        const rows = (await loadFile(folder, file)).filter(row => row.Date <= date && filter(row));
        if (rows.length) {
            const day = rows.reduce((latest, row) => (row.Date > latest ? row.Date : latest), '');
            return {date: day, rows: rows.filter(row => row.Date === day)};
        }
    }
    return {date: null, rows: []};
}

function loadBroadcasts() {
    return loadFolder('tv');
}

// Everything the title pages need, indexed by title key. Loaded once per visit (~0.8 MB gzipped).
let historyPromise = null;

function loadHistory() {
    if (!historyPromise) {
        historyPromise = (async () => {
            const manifest = await loadManifest();
            const [online, totaltv, tv] = await Promise.all([loadFolder('online'), loadFolder('totaltv'), loadFolder('tv')]);
            const titles = new Map();
            const add = (source, row) => {
                const key = titleKey(row.Program);
                if (!key) return;
                if (!titles.has(key)) {
                    titles.set(key, {key, names: new Map(), online: [], totaltv: [], tv: []});
                }
                const title = titles.get(key);
                title[source].push(row);
                const name = seriesName(row.Program);
                title.names.set(name, (title.names.get(name) || 0) + 1);
            };
            totaltv.forEach(row => add('totaltv', row));
            online.forEach(row => add('online', row));
            tv.forEach(row => add('tv', row));
            titles.forEach(title => {
                // The most used spelling, preferring a name that was not cut off
                title.name = [...title.names.entries()]
                    .sort((a, b) => b[1] - a[1] || b[0].length - a[0].length)[0][0];
                // What the search shows next to the name
                const dates = [...title.totaltv, ...title.online, ...title.tv].map(row => row.Date).filter(Boolean);
                title.lastDate = dates.reduce((latest, day) => (day > latest ? day : latest), '');
                title.where = [...new Set([...title.totaltv.map(row => row.Group), ...title.online.map(row => row.Service),
                    ...title.tv.map(row => row.Channel)])];
            });
            const listDates = {
                online: [...new Set(online.map(row => row.Date))].sort(),
                totaltv: [...new Set(totaltv.map(row => row.Date))].sort(),
            };
            return {manifest, titles, online, totaltv, tv, listDates};
        })();
        historyPromise.catch(() => { historyPromise = null; });
    }
    return historyPromise;
}

// ---------------------------------------------------------------- small shared helpers

function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = text;
    return node;
}

function link(href, text, className) {
    const anchor = el('a', className, text);
    anchor.href = href;
    return anchor;
}

// Numbers the Finnish way: 1 023 000, 84 t., 1,02 milj., 7 %
const NBSP = '\u00a0';

function thousands(value) {
    return `${Math.round(value / 1000)}${NBSP}t.`;
}

function compact(value) {
    if (value >= 1e6) return `${(value / 1e6).toLocaleString('fi-FI', {maximumFractionDigits: 2})}${NBSP}milj.`;
    if (value >= 1000) return thousands(value);
    return String(value);
}

function percent(share) {
    return `${Math.round(share * 100)}${NBSP}%`;
}

function fullNumber(value) {
    return Number(value).toLocaleString('fi-FI');
}

function decimal(value, digits = 2) {
    return Number(value).toLocaleString('fi-FI', {minimumFractionDigits: digits, maximumFractionDigits: digits});
}

function isoDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

// Calendar dates as local midnight, so no time zone moves a day (new Date('YYYY-MM-DD') is UTC)
function parseIso(text) {
    const [year, month, day] = String(text).split('-').map(Number);
    return new Date(year, month - 1, day);
}

function addDays(text, days) {
    const date = parseIso(text);
    date.setDate(date.getDate() + days);
    return isoDate(date);
}

function finnishDate(text, withYear = true) {
    const date = parseIso(text);
    return `${date.getDate()}.${date.getMonth() + 1}.${withYear ? date.getFullYear() : ''}`;
}

function windowText(start, end) {
    return `${finnishDate(start, false)}–${finnishDate(end)}`;
}

const WEEKDAY_NAMES = {ma: 'ma', ti: 'ti', ke: 'ke', to: 'to', pe: 'pe', la: 'la', su: 'su'};

// The window a list covers. Lists collected since 2026-11-24 carry it; the xlsx archive did not,
// and for those it is the 14 or 90 days ending two days before the list date, as the headers
// showed on the days checked (2026-08-21 and 2026-11-24)
function onlineWindow(date, period) {
    const end = addDays(date, -2);
    return {start: addDays(end, period === '90D' ? -89 : -13), end};
}

function rowWindow(row) {
    if (row.PeriodStart && row.PeriodEnd) return {start: row.PeriodStart, end: row.PeriodEnd};
    return onlineWindow(row.Date, row.Period);
}

function downloadSheets(filename, sheets) {
    const workbook = XLSX.utils.book_new();
    sheets.forEach(({name, rows}) => {
        XLSX.utils.book_append_sheet(workbook, XLSX.utils.json_to_sheet(rows.length ? rows : [{}]), name);
    });
    XLSX.writeFile(workbook, filename);
}
