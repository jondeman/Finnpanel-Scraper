// Total TV tab. A TotalTV list counts a programme's most-watched episode over 14 days on TV and
// online together. Each row is joined to the same programme's online-only figure for the same
// 14 days (the 14D list scraped in the same run) and to its biggest linear airing in the window.

let tvSort = 'total';
let tvShowAll = false;

function bestAiringViewers(airing) {
    return Number(airing.Viewers7d || airing.ViewersSameDay || 0);
}

function enrichTotalTv(rows, onlineRows, broadcasts) {
    const online = new Map();
    const floors = {};
    onlineRows.forEach(row => {
        const group = groupKeyOf(row.Service);
        const viewers = Number(row.Viewers);
        const listKey = `${row.Demo}|${group}`;
        floors[listKey] = Math.min(floors[listKey] ?? Infinity, viewers);
        const key = `${listKey}|${titleKey(row.Program)}`;
        const current = online.get(key);
        if (!current || Number(row.ServiceRank || row.Rank) < current.rank) {
            online.set(key, {viewers, rank: Number(row.ServiceRank || row.Rank), service: row.Service, program: String(row.Program), episode: row.Episode || ''});
        }
    });
    const airingsByTitle = new Map();
    broadcasts.forEach(airing => {
        const key = `${airing.Group}|${titleKey(airing.Program)}`;
        if (!airingsByTitle.has(key)) airingsByTitle.set(key, []);
        airingsByTitle.get(key).push(airing);
    });

    rows.forEach(row => {
        const group = groupKeyOf(row.Service);
        const key = titleKey(row.Program);
        const total = Number(row.Viewers);
        const match = online.get(`${row.Demo}|${group}|${key}`);
        row.online = match || null;
        row.onlineCeiling = !match && onlineRows.length ? floors[`${row.Demo}|${group}`] ?? null : null;
        if (match) {
            // Both lists name their most-watched episode; when those are known and differ, or the
            // online figure is the bigger one, the share compares two different episodes
            const totalEpisode = row.Episode || (ruutuEpisode(row.Program) ? row.Program : '');
            const onlineEpisode = match.episode || (ruutuEpisode(match.program) ? match.program : '');
            const differs = totalEpisode && onlineEpisode && totalEpisode.slice(0, 40) !== onlineEpisode.slice(0, 40);
            row.onlineShare = Math.min(match.viewers / total, 1);
            row.onlineShareApprox = match.viewers > total || Boolean(differs);
        } else {
            row.onlineShare = null;
            row.onlineShareApprox = false;
        }
        const airings = (airingsByTitle.get(`${row.Service}|${key}`) || [])
            .filter(airing => airing.Date >= row.PeriodStart && airing.Date <= row.PeriodEnd);
        row.airings = airings;
        row.bestAiring = airings.length
            ? airings.reduce((best, airing) => bestAiringViewers(airing) > bestAiringViewers(best) ? airing : best)
            : null;
    });
}

function updateTvChart() {
    const demo = selectedDemo();
    const rows = state.rows.filter(row => row.Demo === demo && inService(row.Service));
    const comparers = {
        total: (a, b) => b.Viewers - a.Viewers,
        online: (a, b) => (b.online ? b.online.viewers : -1) - (a.online ? a.online.viewers : -1) || b.Viewers - a.Viewers,
        share: (a, b) => (b.onlineShare ?? -1) - (a.onlineShare ?? -1) || b.Viewers - a.Viewers,
    };
    rows.sort(comparers[tvSort]);

    const shares = rows.filter(row => row.onlineShare !== null).map(row => row.onlineShare).sort((a, b) => a - b);
    const median = shares.length ? shares[Math.floor(shares.length / 2)] : null;
    const first = rows[0];
    const summary = document.getElementById('tvSummary');
    summary.textContent = first
        ? [`TV + online, ${windowText(first.PeriodStart, first.PeriodEnd)}`, `${rows.length} programmes`,
            median !== null ? `online is a median ${percent(median)} of the audience (${shares.length} programmes in both lists)` : null]
            .filter(Boolean).join(' · ')
        : '';

    const list = document.getElementById('tvBars');
    list.innerHTML = '';
    const scale = Math.max(1, ...rows.map(row => Number(row.Viewers)));
    (tvShowAll ? rows : rows.slice(0, topRows)).forEach((row, index) => list.appendChild(renderTvRow(row, scale, index)));
    if (!rows.length) list.appendChild(el('li', 'empty-cell', 'No programmes in this list.'));

    const more = document.getElementById('tvShowAll');
    more.hidden = rows.length <= topRows;
    more.textContent = tvShowAll ? `Show top ${topRows}` : `Show all ${rows.length} programmes`;
}

function renderTvRow(row, scale, index) {
    const li = el('li', 'tv-row');
    const total = Number(row.Viewers);
    const pos = el('span', 'pos', String(row.Rank));
    pos.dataset.tip = `#${row.Rank} across Yle, MTV and Sanoma · #${row.GroupRank} at ${row.Service}`;

    const meta = [`${row.Service} #${row.GroupRank}`];
    if (row.Episode) meta.push(row.Episode);
    const who = el('span', 'who');
    who.append(link(titleHref(row.Program), seriesName(row.Program), 'name title-link'), el('span', 'meta', meta.join(' · ')));

    const area = el('span', 'tv-bar-area');
    const bar = el('span', 'tv-bar');
    bar.style.setProperty('--w', String(total / scale));
    bar.style.setProperty('--delay', `${Math.min(index, topRows) * 25}ms`);
    const onlineViewers = row.online ? Math.min(row.online.viewers, total) : 0;
    if (onlineViewers > 0) {
        const seg = el('span', 'tv-seg seg-online');
        seg.style.flex = `${onlineViewers} 1 0`;
        bar.appendChild(seg);
    }
    if (total - onlineViewers > 0) {
        const seg = el('span', 'tv-seg seg-tv');
        seg.style.flex = `${total - onlineViewers} 1 0`;
        bar.appendChild(seg);
    }
    const label = el('span', 'val');
    label.appendChild(el('strong', null, thousands(total)));
    const share = row.onlineShare !== null
        ? `${row.onlineShareApprox ? '≈' : ''}${percent(row.onlineShare)} online`
        : row.onlineCeiling ? `online ≤${thousands(row.onlineCeiling)}` : '';
    if (share) label.appendChild(el('span', 'share', share));
    area.append(bar, label);

    const tip = [`TV + online: ${fullNumber(total)}`];
    if (row.online) {
        tip.push(`Online only: ${fullNumber(row.online.viewers)} (#${row.online.rank} on ${row.online.service})${row.onlineShareApprox ? ', a different episode' : ''}`);
    } else if (row.onlineCeiling) {
        tip.push(`Not in the online top 20: at most ${fullNumber(row.onlineCeiling)} online`);
    }
    if (row.bestAiring) {
        tip.push(`Best linear airing: ${airingText(row.bestAiring)}${row.bestAiring.Viewers7d ? ' (7-day)' : ' (same day)'}`);
    }
    area.dataset.tip = tip.join(' · ');

    li.append(pos, who, area);
    return li;
}
