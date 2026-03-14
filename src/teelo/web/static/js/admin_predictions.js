const TOUR_COLORS = {
    ATP: '#002865', // var(--tour-atp)
    WTA: '#E30066', // var(--tour-wta)
    Challenger: '#006B3F', // var(--tour-challenger)
    ITF: '#6B7280', // var(--tour-itf)
    'WTA 125': '#9CA3AF' // --content-faintest approx
};

const CHART_COLORS = {
    primary: '#CCFF00', // var(--color-teelo-lime)
    secondary: '#1A1A1A', // var(--color-teelo-dark)
    grid: '#E5E7EB', // --line approx
    ideal: '#9CA3AF' // --content-faintest approx
};

const chartInstances = {};

function formatPct(v) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) {
        return '—';
    }
    return `${(Number(v) * 100).toFixed(1)}%`;
}

function formatDelta(v, inverse = false) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) {
        return '';
    }

    const value = Number(v);
    const isNegative = value < 0;
    const isGood = inverse ? isNegative : !isNegative;
    const arrow = isNegative ? '↓' : '↑';
    const color = isGood ? 'text-status-success' : 'text-status-danger';

    return `<span class="${color} font-semibold">${arrow} ${Math.abs(value * 100).toFixed(1)}%</span>`;
}

function formatNumber(v, digits = 4) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) {
        return '—';
    }
    return Number(v).toFixed(digits);
}

function setElementText(id, selector, value) {
    const card = document.getElementById(id);
    if (!card) {
        return;
    }
    const target = card.querySelector(selector);
    if (!target) {
        return;
    }
    target.textContent = value;
}

function setElementHTML(id, selector, value) {
    const card = document.getElementById(id);
    if (!card) {
        return;
    }
    const target = card.querySelector(selector);
    if (!target) {
        return;
    }
    target.innerHTML = value;
}

function setSummaryLoading(source) {
    ['accuracy', 'log-loss', 'ece', 'brier', 'matches'].forEach((name) => {
        const id = `${source}-card-${name}`;
        setElementText(id, '[data-role="value"]', 'Loading...');
        setElementText(id, '[data-role="delta"]', '');
    });
}

function setSummaryError(source) {
    ['accuracy', 'log-loss', 'ece', 'brier', 'matches'].forEach((name) => {
        const id = `${source}-card-${name}`;
        setElementText(id, '[data-role="value"]', 'Error loading data');
        setElementText(id, '[data-role="delta"]', '');
    });
}

function setLoading(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = '<p class="text-content-faint text-sm py-4">Loading...</p>';
    }
}

function setError(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = '<p class="text-status-danger text-sm py-4">Error loading data</p>';
    }
}

function computeRollingAverage(values, window) {
    return values.map((_, index) => {
        if (index < 2) {
            return null;
        }

        const start = Math.max(0, index - window + 1);
        const slice = values.slice(start, index + 1).filter((v) => v !== null && v !== undefined && !Number.isNaN(Number(v)));
        if (slice.length < 3) {
            return null;
        }

        const total = slice.reduce((sum, value) => sum + Number(value), 0);
        return total / slice.length;
    });
}

function destroyChart(id) {
    if (chartInstances[id]) {
        chartInstances[id].destroy();
        delete chartInstances[id];
    }
}

function renderAccuracyChart(source, dailyData) {
    const id = `chart-accuracy-time-${source}`;
    const canvas = document.getElementById(id);
    if (!canvas || !Array.isArray(dailyData)) {
        return;
    }

    const sorted = [...dailyData].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    const labels = sorted.map((row) => row.date);
    const values = sorted.map((row) => (row.accuracy === null || row.accuracy === undefined ? null : Number(row.accuracy) * 100));
    const rolling = computeRollingAverage(values, 7);

    destroyChart(id);
    chartInstances[id] = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Daily Accuracy',
                    data: values,
                    borderColor: CHART_COLORS.primary,
                    backgroundColor: CHART_COLORS.primary,
                    tension: 0.3,
                    spanGaps: true
                },
                {
                    label: '7-day Rolling Avg',
                    data: rolling,
                    borderColor: CHART_COLORS.ideal,
                    backgroundColor: CHART_COLORS.ideal,
                    tension: 0.4,
                    spanGaps: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    min: 40,
                    max: 100,
                    title: {
                        display: true,
                        text: 'Accuracy (%)'
                    },
                    grid: {
                        color: CHART_COLORS.grid
                    }
                },
                x: {
                    grid: {
                        color: CHART_COLORS.grid
                    }
                }
            }
        }
    });
}

function renderCalibrationChart(source, calibrationData) {
    const id = `chart-calibration-${source}`;
    const canvas = document.getElementById(id);
    if (!canvas || !Array.isArray(calibrationData)) {
        return;
    }

    const sorted = [...calibrationData].sort((a, b) => Number(a.bin_center) - Number(b.bin_center));
    const points = sorted.map((row) => ({ x: Number(row.bin_center), y: Number(row.actual_win_rate) }));
    const ideal = sorted.map((row) => {
        const x = Number(row.bin_center);
        return { x, y: x };
    });

    destroyChart(id);
    chartInstances[id] = new Chart(canvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Actual Win Rate',
                    data: points,
                    borderColor: CHART_COLORS.primary,
                    backgroundColor: CHART_COLORS.primary,
                    tension: 0.25
                },
                {
                    label: 'Perfect Calibration',
                    data: ideal,
                    borderColor: CHART_COLORS.ideal,
                    backgroundColor: CHART_COLORS.ideal,
                    borderDash: [6, 6],
                    pointRadius: 0
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            parsing: false,
            scales: {
                x: {
                    min: 0,
                    max: 1,
                    title: {
                        display: true,
                        text: 'Predicted Probability'
                    },
                    grid: {
                        color: CHART_COLORS.grid
                    }
                },
                y: {
                    min: 0,
                    max: 1,
                    title: {
                        display: true,
                        text: 'Actual Win Rate'
                    },
                    grid: {
                        color: CHART_COLORS.grid
                    }
                }
            }
        }
    });
}

function renderDistributionChart(source, confidenceData) {
    const id = `chart-confidence-dist-${source}`;
    const canvas = document.getElementById(id);
    if (!canvas || !Array.isArray(confidenceData)) {
        return;
    }

    const labels = confidenceData.map((row) => row.label ?? row.bucket ?? row.confidence_bucket ?? 'Unknown');
    const values = confidenceData.map((row) => Number(row.n_matches ?? 0));

    destroyChart(id);
    chartInstances[id] = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Matches',
                    data: values,
                    backgroundColor: CHART_COLORS.primary,
                    borderColor: CHART_COLORS.primary
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    grid: {
                        color: CHART_COLORS.grid
                    }
                },
                x: {
                    grid: {
                        color: CHART_COLORS.grid
                    }
                }
            }
        }
    });
}

function renderTourAccuracyChart(source, tourData) {
    const id = `chart-tour-accuracy-${source}`;
    const canvas = document.getElementById(id);
    if (!canvas || !tourData || typeof tourData !== 'object') {
        return;
    }

    const tours = Object.keys(tourData);
    const values = tours.map((tour) => Number(tourData[tour].accuracy ?? 0) * 100);
    const colors = tours.map((tour) => TOUR_COLORS[tour] || CHART_COLORS.secondary);

    destroyChart(id);
    chartInstances[id] = new Chart(canvas, {
        type: 'bar',
        data: {
            labels: tours,
            datasets: [
                {
                    label: 'Accuracy (%)',
                    data: values,
                    backgroundColor: colors,
                    borderColor: colors
                }
            ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    min: 0,
                    max: 100,
                    grid: {
                        color: CHART_COLORS.grid
                    }
                },
                y: {
                    grid: {
                        color: CHART_COLORS.grid
                    }
                }
            }
        }
    });
}

function renderBreakdownTable(containerId, data, columns) {
    const container = document.getElementById(containerId);
    if (!container) {
        return;
    }

    if (!data || Object.keys(data).length === 0) {
        container.innerHTML = '<p class="text-content-muted text-sm py-4">No data available</p>';
        return;
    }

    const headers = ['Category', ...columns.map((col) => col.label)];
    const rows = Object.entries(data).map(([category, values]) => {
        const cells = columns.map((col) => {
            const raw = values ? values[col.key] : null;
            if (typeof col.format === 'function') {
                const formatted = col.format(raw);
                return formatted === null || formatted === undefined || formatted === '' ? '—' : formatted;
            }
            if (raw === null || raw === undefined || raw === '') {
                return '—';
            }
            return raw;
        });
        return [category, ...cells];
    });

    const thead = `<thead><tr class="border-b border-line-subtle text-left text-xs uppercase tracking-wide text-content-muted">${headers
        .map((header) => `<th class="py-2 pr-3 font-semibold">${header}</th>`)
        .join('')}</tr></thead>`;

    const tbody = `<tbody>${rows
        .map(
            (row) =>
                `<tr class="border-b border-line-subtle">${row
                    .map((cell, index) => `<td class="py-2 pr-3 ${index === 0 ? 'font-medium text-teelo-dark' : 'text-content-secondary'}">${cell}</td>`)
                    .join('')}</tr>`
        )
        .join('')}</tbody>`;

    container.innerHTML = `<table class="w-full text-sm">${thead}${tbody}</table>`;
}

async function loadSummary(source) {
    setSummaryLoading(source);

    try {
        const response = await fetch('/admin/api/predictions/summary');
        if (!response.ok) {
            throw new Error('Summary request failed');
        }
        const payload = await response.json();
        const data = payload[source] || {};

        setElementText(`${source}-card-accuracy`, '[data-role="value"]', formatPct(data.accuracy));
        setElementHTML(`${source}-card-accuracy`, '[data-role="delta"]', formatDelta(data.delta_accuracy));

        setElementText(`${source}-card-log-loss`, '[data-role="value"]', formatNumber(data.log_loss));
        setElementHTML(`${source}-card-log-loss`, '[data-role="delta"]', formatDelta(data.delta_log_loss, true));

        setElementText(`${source}-card-ece`, '[data-role="value"]', formatNumber(data.ece));
        setElementText(`${source}-card-ece`, '[data-role="delta"]', '');

        setElementText(`${source}-card-brier`, '[data-role="value"]', formatNumber(data.brier_score));
        setElementText(`${source}-card-brier`, '[data-role="delta"]', '');

        setElementText(`${source}-card-matches`, '[data-role="value"]', data.n_matches ?? '—');
        setElementText(`${source}-card-matches`, '[data-role="delta"]', '');
    } catch (error) {
        setSummaryError(source);
    }
}

async function loadCharts(source) {
    const chartIds = [
        `chart-accuracy-time-${source}`,
        `chart-calibration-${source}`,
        `chart-confidence-dist-${source}`,
        `chart-tour-accuracy-${source}`
    ];

    chartIds.forEach((id) => {
        destroyChart(id);
        const canvas = document.getElementById(id);
        if (canvas) {
            const ctx = canvas.getContext('2d');
            if (ctx) {
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.fillStyle = '#9CA3AF'; // --content-faintest approx
                ctx.font = '14px Inter, sans-serif';
                ctx.fillText('Loading...', 12, 22);
            }
        }
    });

    try {
        const [accuracyResponse, calibrationResponse, distributionResponse] = await Promise.all([
            fetch(`/admin/api/predictions/charts/accuracy?source=${source}`),
            fetch(`/admin/api/predictions/charts/calibration?source=${source}`),
            fetch(`/admin/api/predictions/charts/distribution?source=${source}`)
        ]);

        if (!accuracyResponse.ok || !calibrationResponse.ok || !distributionResponse.ok) {
            throw new Error('Chart request failed');
        }

        const accuracyData = await accuracyResponse.json();
        const calibrationData = await calibrationResponse.json();
        const distributionData = await distributionResponse.json();

        // Convert daily object {date: {n_matches, accuracy}} to array
        const dailyObj = accuracyData.daily || {};
        const dailyArr = Object.entries(dailyObj).map(([date, v]) => ({date, ...v}));
        renderAccuracyChart(source, dailyArr);
        renderCalibrationChart(source, calibrationData.calibration || []);
        // Convert confidence object to array
        const confObj = distributionData.by_confidence || {};
        const confArr = Object.entries(confObj).map(([label, v]) => ({label, ...v}));
        renderDistributionChart(source, confArr);
        renderTourAccuracyChart(source, accuracyData.by_tour || {});
    } catch (error) {
        chartIds.forEach((id) => {
            destroyChart(id);
            const canvas = document.getElementById(id);
            if (canvas) {
                const ctx = canvas.getContext('2d');
                if (ctx) {
                    ctx.clearRect(0, 0, canvas.width, canvas.height);
                    ctx.fillStyle = '#F87171'; // status-danger light
                    ctx.font = '14px Inter, sans-serif';
                    ctx.fillText('Error loading data', 12, 22);
                }
            }
        });
    }
}

async function loadBreakdown(source) {
    const tableIds = ['tour', 'surface', 'level', 'round', 'confidence'].map((name) => `${source}-table-${name}`);
    tableIds.forEach(setLoading);

    try {
        const response = await fetch(`/admin/api/predictions/breakdown?source=${source}`);
        if (!response.ok) {
            throw new Error('Breakdown request failed');
        }

        const payload = await response.json();
        const standardColumns = [
            { key: 'n_matches', label: 'Matches' },
            { key: 'accuracy', label: 'Accuracy', format: formatPct },
            { key: 'log_loss', label: 'Log Loss', format: (v) => formatNumber(v, 4) },
            { key: 'brier_score', label: 'Brier', format: (v) => formatNumber(v, 4) }
        ];

        const confidenceColumns = [
            { key: 'n_matches', label: 'Matches' },
            { key: 'avg_confidence', label: 'Avg Confidence', format: formatPct },
            { key: 'actual_win_rate', label: 'Actual Win Rate', format: formatPct },
            { key: 'accuracy', label: 'Accuracy', format: formatPct }
        ];

        renderBreakdownTable(`${source}-table-tour`, payload.by_tour, standardColumns);
        renderBreakdownTable(`${source}-table-surface`, payload.by_surface, standardColumns);
        renderBreakdownTable(`${source}-table-level`, payload.by_level, standardColumns);
        renderBreakdownTable(`${source}-table-round`, payload.by_round, standardColumns);
        renderBreakdownTable(`${source}-table-confidence`, payload.by_confidence, confidenceColumns);
    } catch (error) {
        tableIds.forEach(setError);
    }
}

function loadAll(source) {
    return Promise.all([loadSummary(source), loadCharts(source), loadBreakdown(source)]);
}

function setTabActive(tab, active) {
    if (!tab) {
        return;
    }

    if (active) {
        tab.classList.remove('text-content-muted');
        tab.classList.add('border-b-2', 'border-[#CCFF00]', 'text-[#CCFF00]');
        tab.style.borderBottomColor = '#CCFF00'; // var(--color-teelo-lime)
        tab.style.color = '#CCFF00'; // var(--color-teelo-lime)
        return;
    }

    tab.classList.remove('border-b-2', 'border-[#CCFF00]', 'text-[#CCFF00]');
    tab.classList.add('text-content-muted');
    tab.style.borderBottomColor = 'transparent';
    tab.style.color = '';
}

document.addEventListener('DOMContentLoaded', () => {
    const tabs = Array.from(document.querySelectorAll('[data-tab]'));
    const contents = ['live', 'backfill', 'cv'].map((tab) => document.getElementById(`tab-content-${tab}`));

    tabs.forEach((tab) => {
        tab.addEventListener('click', () => {
            tabs.forEach((candidate) => {
                setTabActive(candidate, false);
            });

            setTabActive(tab, true);

            contents.forEach((content) => {
                if (content) {
                    content.classList.add('hidden');
                }
            });

            const selected = tab.getAttribute('data-tab');
            const panel = document.getElementById(`tab-content-${selected}`);
            if (panel) {
                panel.classList.remove('hidden');
            }

            if (selected === 'live') {
                loadAll('live');
            }
            if (selected === 'backfill') {
                loadAll('backfill');
            }
        });
    });

    if (tabs.length > 0) {
        tabs[0].click();
    }
});
