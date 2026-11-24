// Time-series chart in SVG. One y-axis (viewers). Lines for daily list figures, broken where a
// programme drops out of a list rather than bridged; dots for single broadcasts. A crosshair snaps
// to the nearest date and one tooltip lists every series there. Every value is also in the
// title page's tables, so the tooltip never gates anything.
const SVG_NS = 'http://www.w3.org/2000/svg';
const DAY_MS = 86400000;

function svg(tag, attributes = {}) {
    const node = document.createElementNS(SVG_NS, tag);
    Object.entries(attributes).forEach(([name, value]) => node.setAttribute(name, value));
    return node;
}

function niceStep(max, count) {
    const raw = max / count;
    const power = 10 ** Math.floor(Math.log10(raw));
    return [1, 2, 2.5, 5, 10].map(multiple => multiple * power).find(step => step >= raw);
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function timeTicks(from, to, width) {
    const days = (to - from) / DAY_MS;
    const ticks = [];
    if (days > 75) {
        // A divisor of 12, so ticks fall on the same months every year and stay evenly spaced
        const needed = (days / 30) / Math.max(2, width / 90);
        const monthsPerTick = [1, 2, 3, 6, 12].find(step => step >= needed) || 12;
        const date = new Date(from);
        date.setDate(1);
        date.setMonth(date.getMonth() + 1);
        while (date <= to) {
            if (date.getMonth() % monthsPerTick === 0) {
                const label = date.getMonth() === 0 || ticks.length === 0
                    ? `${MONTHS[date.getMonth()]} ${date.getFullYear()}` : MONTHS[date.getMonth()];
                ticks.push({t: date.getTime(), label});
            }
            date.setMonth(date.getMonth() + 1);
        }
    } else {
        const step = [1, 2, 7, 14].find(candidate => days / candidate <= Math.max(2, width / 100)) || 28;
        const date = new Date(from);
        if (step >= 7) date.setDate(date.getDate() + ((8 - date.getDay()) % 7)); // Mondays
        while (date <= to) {
            ticks.push({t: date.getTime(), label: `${date.getDate()}.${date.getMonth() + 1}.`});
            date.setDate(date.getDate() + step);
        }
    }
    return ticks;
}

/**
 * series: [{id, label, color, kind: 'line'|'dots', gapDays, points: [{t, y, lines: [string]}]}]
 * options: {from, to (ms), height, emptyText, describe (accessible summary)}
 */
function renderTimeChart(container, series, options = {}) {
    container.innerHTML = '';
    container.classList.add('tchart');
    const visible = series.map(item => ({
        ...item,
        points: item.points
            .filter(point => point.t >= options.from && point.t <= options.to && point.y !== null)
            .sort((a, b) => a.t - b.t),
    }));

    const legend = el('div', 'tchart-legend');
    visible.forEach(item => {
        const entry = el('span', 'tchart-key');
        const swatch = el('i', item.kind === 'dots' ? 'key-dot' : 'key-line');
        swatch.style.background = item.color;
        entry.append(swatch, document.createTextNode(item.label));
        if (!item.points.length) {
            entry.classList.add('empty');
            entry.title = 'No figures in this range';
        }
        legend.appendChild(entry);
    });
    container.appendChild(legend);

    const all = visible.flatMap(item => item.points);
    if (!all.length) {
        container.appendChild(el('p', 'tchart-empty', options.emptyText || 'No figures in this range.'));
        return;
    }

    const width = Math.max(300, container.clientWidth || 600);
    const height = options.height || 280;
    const margin = {top: 14, right: width < 480 ? 44 : 60, bottom: 26, left: 46};
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const from = options.from;
    const to = Math.max(options.to, from + DAY_MS);
    const maxY = Math.max(...all.map(point => point.y));
    const yStep = niceStep(maxY * 1.05 || 1, 4);
    const yMax = Math.ceil((maxY * 1.05 || 1) / yStep) * yStep;
    const x = t => margin.left + ((t - from) / (to - from)) * plotWidth;
    const y = value => margin.top + plotHeight - (value / yMax) * plotHeight;

    const root = svg('svg', {width, height, viewBox: `0 0 ${width} ${height}`, role: 'img'});
    root.setAttribute('aria-label', options.describe || visible.map(item => item.label).join(', '));
    const grid = svg('g', {class: 'tchart-grid'});
    for (let value = 0; value <= yMax + 1; value += yStep) {
        grid.appendChild(svg('line', {x1: margin.left, x2: width - margin.right, y1: y(value), y2: y(value)}));
        const label = svg('text', {x: margin.left - 8, y: y(value) + 4, 'text-anchor': 'end', class: 'tchart-tick'});
        label.textContent = compact(value);
        grid.appendChild(label);
    }
    timeTicks(new Date(from), new Date(to), plotWidth).forEach(tick => {
        const label = svg('text', {x: x(tick.t), y: height - 6, 'text-anchor': 'middle', class: 'tchart-tick'});
        label.textContent = tick.label;
        grid.appendChild(label);
    });
    grid.appendChild(svg('line', {
        x1: margin.left, x2: width - margin.right, y1: y(0), y2: y(0), class: 'tchart-baseline',
    }));
    root.appendChild(grid);

    const endLabels = [];
    visible.forEach(item => {
        const group = svg('g');
        if (!item.points.length) return;
        if (item.kind === 'line') {
            // Split into runs wherever the programme was out of the list for more than gapDays
            const runs = [];
            item.points.forEach(point => {
                const run = runs[runs.length - 1];
                if (run && point.t - run[run.length - 1].t <= (item.gapDays || 2) * DAY_MS) {
                    run.push(point);
                } else {
                    runs.push([point]);
                }
            });
            runs.forEach(run => {
                if (run.length === 1) {
                    group.appendChild(svg('circle', {cx: x(run[0].t), cy: y(run[0].y), r: 3.5, fill: item.color}));
                } else {
                    const d = run.map((point, index) => `${index ? 'L' : 'M'}${x(point.t).toFixed(1)},${y(point.y).toFixed(1)}`).join('');
                    group.appendChild(svg('path', {d, fill: 'none', stroke: item.color, 'stroke-width': 2,
                        'stroke-linejoin': 'round', 'stroke-linecap': 'round'}));
                }
            });
            const last = item.points[item.points.length - 1];
            endLabels.push({x: x(last.t), y: y(last.y), text: compact(last.y)});
        } else {
            item.points.forEach(point => {
                group.appendChild(svg('circle', {cx: x(point.t), cy: y(point.y), r: 4.5, fill: item.color,
                    class: 'tchart-dot'}));
            });
        }
        root.appendChild(group);
    });

    // Value at each line's end; a label that would collide with one already placed is left to the tooltip
    const placed = [];
    endLabels.sort((a, b) => a.y - b.y).forEach(label => {
        if (placed.some(other => Math.abs(other.y - label.y) < 14 && Math.abs(other.x - label.x) < 50)) return;
        placed.push(label);
        const text = svg('text', {x: label.x + 7, y: label.y + 4, class: 'tchart-end'});
        text.textContent = label.text;
        root.appendChild(text);
    });

    // Crosshair: snaps to the nearest date that has any figure
    const times = [...new Set(all.map(point => point.t))].sort((a, b) => a - b);
    const crosshair = svg('line', {y1: margin.top, y2: margin.top + plotHeight, class: 'tchart-crosshair', visibility: 'hidden'});
    const focusDots = svg('g');
    root.append(crosshair, focusDots);
    const overlay = svg('rect', {x: margin.left, y: 0, width: plotWidth + 8, height, fill: 'transparent'});
    root.appendChild(overlay);
    container.appendChild(root);

    const tooltip = el('div', 'tchart-tip');
    tooltip.hidden = true;
    container.appendChild(tooltip);
    container.tabIndex = 0;
    let current = -1;

    function show(index) {
        current = Math.max(0, Math.min(times.length - 1, index));
        const t = times[current];
        crosshair.setAttribute('x1', x(t));
        crosshair.setAttribute('x2', x(t));
        crosshair.setAttribute('visibility', 'visible');
        focusDots.innerHTML = '';
        tooltip.innerHTML = '';
        const date = new Date(t);
        tooltip.appendChild(el('div', 'tip-date', `${WEEKDAYS_EN[date.getDay()]} ${date.getDate()}.${date.getMonth() + 1}.${date.getFullYear()}`));
        visible.forEach(item => {
            const here = item.points.filter(point => point.t === t);
            const row = el('div', 'tip-row');
            const key = el('i', 'key-line');
            key.style.background = item.color;
            if (!here.length) {
                if (item.kind === 'dots') return; // airings are on single days; no row on other days
                row.append(key, el('span', 'tip-missing', `${item.label}: not in the list`));
                tooltip.appendChild(row);
                return;
            }
            here.forEach(point => {
                focusDots.appendChild(svg('circle', {cx: x(t), cy: y(point.y), r: 5, fill: item.color, class: 'tchart-dot'}));
                const line = el('div', 'tip-row');
                const keyCopy = key.cloneNode();
                const body = el('span');
                body.append(el('strong', null, fullNumber(point.y)), document.createTextNode(` ${item.label}`));
                (point.lines || []).forEach(text => body.appendChild(el('span', 'tip-sub', text)));
                line.append(keyCopy, body);
                tooltip.appendChild(line);
            });
        });
        tooltip.hidden = false;
        const left = x(t) + 14;
        tooltip.style.left = `${left + tooltip.offsetWidth > width ? x(t) - tooltip.offsetWidth - 14 : left}px`;
        tooltip.style.top = `${margin.top + legend.offsetHeight}px`;
    }

    function hide() {
        crosshair.setAttribute('visibility', 'hidden');
        focusDots.innerHTML = '';
        tooltip.hidden = true;
    }

    function nearest(clientX) {
        const box = root.getBoundingClientRect();
        const t = from + ((clientX - box.left - margin.left) / plotWidth) * (to - from);
        let low = 0;
        let high = times.length - 1;
        while (low < high) {
            const mid = (low + high) >> 1;
            if (times[mid] < t) low = mid + 1; else high = mid;
        }
        return low > 0 && t - times[low - 1] < times[low] - t ? low - 1 : low;
    }

    overlay.addEventListener('pointermove', event => show(nearest(event.clientX)));
    overlay.addEventListener('pointerleave', hide);
    container.addEventListener('keydown', event => {
        if (event.key === 'ArrowRight' || event.key === 'ArrowLeft') {
            event.preventDefault();
            show(current < 0 ? times.length - 1 : current + (event.key === 'ArrowRight' ? 1 : -1));
        } else if (event.key === 'Escape') {
            hide();
        }
    });
    container.addEventListener('blur', hide);
}

const WEEKDAYS_EN = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
