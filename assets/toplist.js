// Kärkilista: the top list of either tab, one bar per programme. A Total TV list counts a programme's
// most-watched episode over 14 days on TV and online together; each row is joined to the same
// programme's online-only figure for the same 14 days (the online list of the same day) and to its
// biggest linear airing in the window, and the bar splits into the online part and the rest.
// An online list draws a single bar. A click anywhere on a row opens the programme's history.

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
        const rank = Number(row.ServiceRank || row.Rank);
        if (!current || rank < current.rank) {
            online.set(key, {viewers, rank, service: row.Service, program: String(row.Program), episode: row.Episode || ''});
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

const TOP_EXPLAIN = {
    totaltv: [
        'Total TV on Finnpanelin luku ohjelman katsotuimman jakson keskikatsojamäärästä 14 päivän ajalta, tv:ssä ja verkossa yhteensä. Sija on Ylen, MTV:n ja Nelosen yhteisellä listalla; kanavan oma sija on rivin alla.',
        'Turkoosi osa on sama ohjelma Online-listalla samoilta 14 päivältä (Yle Areena, MTV Katsomo, Ruutu). Kumpikin lista kertoo oman katsotuimman jaksonsa, joten Online-osuus on likimääräinen; ≈ merkitsee tiedossa olevaa eri jaksoa. Jos ohjelma ei ole Online-listan kärki-20:ssä, sen online-katsojamäärä on enintään listan 20. sijan verran.',
        'Vie hiiri palkin päälle nähdäksesi ohjelman parhaan tv-lähetyksen samalta ajalta.',
    ],
    online: [
        'Lista kertoo ohjelman katsotuimman jakson keskikatsojamäärän valitulta ajanjaksolta Yle Areenassa, MTV Katsomossa ja Ruudussa. Sija on kaikkien kolmen palvelun yhteisellä listalla; palvelun oma sija on rivin alla.',
        'Ajanjakso päättyy kaksi päivää ennen listan päivää.',
    ],
};

function updateTopList() {
    const totaltv = state.source === 'totaltv';
    const demo = selectedDemo();
    const rows = state.rows.filter(row => row.Demo === demo && inService(row.Service));
    const comparers = {
        total: (a, b) => b.Viewers - a.Viewers,
        online: (a, b) => (b.online ? b.online.viewers : -1) - (a.online ? a.online.viewers : -1) || b.Viewers - a.Viewers,
        share: (a, b) => (b.onlineShare ?? -1) - (a.onlineShare ?? -1) || b.Viewers - a.Viewers,
    };
    rows.sort(comparers[totaltv ? tvSort : 'total']);

    document.getElementById('topToolbar').hidden = !totaltv;
    const summary = document.getElementById('tvSummary');
    const shares = rows.filter(row => row.onlineShare !== null && row.onlineShare !== undefined).map(row => row.onlineShare).sort((a, b) => a - b);
    summary.textContent = totaltv && shares.length
        ? `Online-osuus katsojista on mediaaniltaan ${percent(shares[Math.floor(shares.length / 2)])} (${shares.length} ohjelmaa molemmilla listoilla).`
        : '';
    summary.hidden = !summary.textContent;

    const explain = document.getElementById('topExplainText');
    explain.innerHTML = '';
    TOP_EXPLAIN[state.source].forEach(text => explain.appendChild(el('p', null, text)));

    const list = document.getElementById('tvBars');
    list.innerHTML = '';
    list.style.setProperty('--label-space', totaltv ? '13em' : '6em');
    const scale = Math.max(1, ...rows.map(row => Number(row.Viewers)));
    (tvShowAll ? rows : rows.slice(0, topRows)).forEach((row, index) => list.appendChild(renderTopRow(row, scale, index, totaltv)));
    if (!rows.length) list.appendChild(el('li', 'empty-cell', 'Tällä valinnalla ei ole ohjelmia.'));

    const more = document.getElementById('tvShowAll');
    more.hidden = rows.length <= topRows;
    more.textContent = tvShowAll ? `Näytä kärki ${topRows}` : `Näytä kaikki ${rows.length} ohjelmaa`;
}

function renderTopRow(row, scale, index, totaltv) {
    const li = el('li', 'tv-row link-row');
    li.tabIndex = 0;
    li.title = `Avaa ${seriesName(row.Program)}: koko historia`;
    li.addEventListener('click', event => {
        if (!event.target.closest('a')) openTitle(row.Program);
    });
    li.addEventListener('keydown', event => {
        if (event.key === 'Enter') openTitle(row.Program);
    });

    const total = Number(row.Viewers);
    const ownRank = row.GroupRank || row.ServiceRank;
    const pos = el('span', 'pos', String(row.Rank));
    pos.dataset.tip = `${row.Rank}. kaikkien kolmen ${totaltv ? 'kanavan' : 'palvelun'} yhteisellä listalla`;

    const meta = [`${genitive(row.Service)} listalla ${ownRank}.`];
    const episode = row.Episode || (ruutuEpisode(row.Program) ? episodeLabel(ruutuEpisode(row.Program)) : '');
    if (episode) meta.push(episode);
    const who = el('span', 'who');
    who.append(link(titleHref(row.Program), seriesName(row.Program), 'name title-link'), el('span', 'meta', meta.join(' · ')));

    const area = el('span', 'tv-bar-area');
    const bar = el('span', 'tv-bar');
    bar.style.setProperty('--w', String(total / scale));
    bar.style.setProperty('--delay', `${Math.min(index, topRows) * 25}ms`);
    const onlineViewers = totaltv ? (row.online ? Math.min(row.online.viewers, total) : 0) : total;
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
    label.appendChild(el('strong', null, fullNumber(total)));
    if (totaltv) {
        const share = row.onlineShare !== null && row.onlineShare !== undefined
            ? `${row.onlineShareApprox ? '≈' : ''}${percent(row.onlineShare)} online`
            : row.onlineCeiling ? `online ≤${thousands(row.onlineCeiling)}` : '';
        if (share) label.appendChild(el('span', 'share', share));
    }
    area.append(bar, label);

    const tip = [`${totaltv ? 'Total TV' : 'Online'}: ${fullNumber(total)} katsojaa`];
    if (totaltv && row.online) {
        tip.push(`online ${fullNumber(row.online.viewers)} (${genitive(row.online.service)} listalla ${row.online.rank}.${row.onlineShareApprox ? ', eri jakso' : ''})`);
    } else if (totaltv && row.onlineCeiling) {
        tip.push(`ei Online-listan kärki-20:ssä, online enintään ${fullNumber(row.onlineCeiling)}`);
    }
    if (totaltv && row.bestAiring) {
        tip.push(`paras tv-lähetys ${airingText(row.bestAiring)} (${row.bestAiring.Viewers7d ? '7 vrk' : 'sama päivä'})`);
    }
    area.dataset.tip = tip.join(' · ');

    li.append(pos, who, area);
    return li;
}
