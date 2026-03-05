import { byId, queryAll, setSectionLoading, toggleHidden } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { escapeHtml, formatShortDate } from '../lib/format.js';
import { buildFallbackCards, buildFallbackTableRows } from '../renderers/matches.js';

export function initPlayerDetailPage() {
  const root = byId('player-page-root');
  if (!root) return;
  const playerId = root.dataset.playerId;
  if (!playerId) return;

  const state = {
    range: 'career',
    surface: 'overall',
    historyChart: null,
    matches: {
      page: 1,
      loading: false,
      hasMore: false,
    },
  };

  function setText(id, value) {
    const el = byId(id);
    if (el) el.textContent = value == null || value === '' ? '—' : String(value);
  }

  function renderRecord(id, block) {
    if (!block) {
      setText(id, null);
      return;
    }
    setText(id, `${block.wins}-${block.losses}`);
  }

  function renderSurfaceCards(items) {
    const container = byId('surface-cards');
    if (!container) return;
    if (!items || !items.length) {
      container.innerHTML = '<p class="text-sm text-gray-400">No surface Elo data yet.</p>';
      return;
    }

    const surfaceAccent = {
      Hard: 'border-t-2 surface-border-hard',
      Clay: 'border-t-2 surface-border-clay',
      Grass: 'border-t-2 surface-border-grass',
      Indoor: 'border-t-2 surface-border-indoor',
    };

    container.innerHTML = items
      .map((row) => {
        const ratingText = row.rating == null ? '—' : row.rating;
        const rankText = row.rank == null ? '—' : `#${row.rank}`;
        const peakText = row.career_peak == null ? '—' : row.career_peak;
        const countText = row.match_count == null ? 0 : row.match_count;
        const accentCls = surfaceAccent[row.surface] || '';

        return `
          <div class="rounded-xl border border-gray-100 p-4 bg-gray-50/40 ${accentCls}">
            <p class="text-[11px] uppercase tracking-wider text-gray-400 font-semibold">${escapeHtml(row.surface || '')}</p>
            <p class="text-2xl font-bold text-teelo-dark mt-1 tabular-nums">${ratingText}</p>
            <p class="text-xs text-gray-500 mt-1">Rank: ${rankText}</p>
            <p class="text-xs text-gray-500 mt-1">Peak: ${peakText}</p>
            <p class="text-xs text-gray-500">Matches: ${countText}</p>
          </div>
        `;
      })
      .join('');
  }

  function renderHistoryChart(data) {
    const points = data?.points || [];
    const canvas = byId('history-chart-canvas');
    const meta = byId('history-meta');
    const minEl = byId('history-min');
    const maxEl = byId('history-max');
    if (!canvas || !meta || !minEl || !maxEl) return;

    if (state.historyChart) {
      state.historyChart.destroy();
      state.historyChart = null;
    }

    if (!points.length) {
      minEl.textContent = 'Min: —';
      maxEl.textContent = 'Max: —';
      meta.textContent = 'No history for selection';
      return;
    }

    const ctx = canvas.getContext('2d');
    const ChartCtor = window.Chart;
    const minRating = Number(data?.min_rating);
    const maxRating = Number(data?.max_rating);

    if (!ctx || typeof ChartCtor === 'undefined') {
      minEl.textContent = `Min: ${Number.isFinite(minRating) ? Math.round(minRating) : '—'}`;
      maxEl.textContent = `Max: ${Number.isFinite(maxRating) ? Math.round(maxRating) : '—'}`;
      meta.textContent = 'Chart library failed to load';
      return;
    }

    let firstTimestamp = Date.now();
    for (let i = 0; i < points.length; i += 1) {
      const parsed = points[i]?.date ? Date.parse(points[i].date) : Number.NaN;
      if (Number.isFinite(parsed)) {
        firstTimestamp = parsed;
        break;
      }
    }

    const oneDayMs = 24 * 60 * 60 * 1000;
    const chartPoints = points.map((p, idx) => {
      const parsedDate = p.date ? Date.parse(p.date) : Number.NaN;
      return {
        x: Number.isFinite(parsedDate) ? parsedDate : firstTimestamp + idx * oneDayMs,
        y: Number(p.rating),
        date: p.date || null,
        match_id: p.match_id,
        tournament_name: p.tournament_name || null,
        round: p.round || null,
        surface: p.surface || null,
        opponent_name: p.opponent_name || null,
        result: p.result || null,
        score: p.score || null,
        elo_change: p.elo_change,
      };
    });

    const realDateValues = chartPoints
      .map((p) => (p.date ? Date.parse(p.date) : Number.NaN))
      .filter((v) => Number.isFinite(v));
    const realMinX = realDateValues.length ? Math.min(...realDateValues) : null;
    const realMaxX = realDateValues.length ? Math.max(...realDateValues) : null;

    state.historyChart = new ChartCtor(ctx, {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'Elo',
            data: chartPoints,
            borderColor: '#1A1A1A',
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            pointHoverBackgroundColor: '#CCFF00',
            pointHoverBorderColor: '#1A1A1A',
            pointHoverBorderWidth: 2,
            tension: 0,
            fill: true,
            backgroundColor(context) {
              const { chart } = context;
              const area = chart.chartArea;
              if (!area) return 'rgba(204, 255, 0, 0.16)';
              const gradient = chart.ctx.createLinearGradient(0, area.top, 0, area.bottom);
              gradient.addColorStop(0, 'rgba(204, 255, 0, 0.25)');
              gradient.addColorStop(1, 'rgba(204, 255, 0, 0)');
              return gradient;
            },
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        parsing: false,
        interaction: {
          mode: 'nearest',
          intersect: false,
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title(items) {
                if (!items.length) return '';
                const raw = items[0].raw || {};
                if (!raw.date) return '';
                const ts = Date.parse(raw.date);
                return Number.isFinite(ts) ? formatShortDate(ts) : '';
              },
              label(item) {
                const raw = item.raw || {};
                const lines = [];
                let eloLine = `Elo ${Math.round(Number(raw.y))}`;
                if (raw.elo_change != null) {
                  const sign = raw.elo_change > 0 ? '+' : '';
                  eloLine += ` (${sign}${raw.elo_change})`;
                }
                if (raw.result) eloLine += ` • ${raw.result}`;
                lines.push(eloLine);

                const eventBits = [];
                if (raw.tournament_name) eventBits.push(raw.tournament_name);
                if (raw.surface) eventBits.push(raw.surface);
                if (raw.round) eventBits.push(raw.round);
                if (eventBits.length) lines.push(eventBits.join(' • '));
                if (raw.opponent_name) lines.push(`vs ${raw.opponent_name}`);

                return lines;
              },
            },
          },
        },
        scales: {
          x: {
            type: 'linear',
            min: realMinX != null ? realMinX : undefined,
            max: realMaxX != null ? realMaxX : undefined,
            title: {
              display: true,
              text: 'Date',
              color: '#6B7280',
              font: { size: 11, weight: '600' },
            },
            grid: { color: 'rgba(15, 17, 21, 0.08)' },
            ticks: {
              maxTicksLimit: 6,
              callback(value) {
                return formatShortDate(Number(value));
              },
            },
          },
          y: {
            title: {
              display: true,
              text: 'Elo',
              color: '#6B7280',
              font: { size: 11, weight: '600' },
            },
            grid: { color: 'rgba(15, 17, 21, 0.08)' },
            ticks: { maxTicksLimit: 6 },
          },
        },
      },
    });

    minEl.textContent = `Min: ${Number.isFinite(minRating) ? Math.round(minRating) : '—'}`;
    maxEl.textContent = `Max: ${Number.isFinite(maxRating) ? Math.round(maxRating) : '—'}`;
    meta.textContent = `Showing ${data?.returned_points || 0} / ${data?.total_points || 0} points`;
  }

  function renderMatches(data, append) {
    const tableBody = byId('player-matches-table-body');
    const cardsContainer = byId('player-matches-cards-container');
    const emptyState = byId('player-matches-empty');
    const countEl = byId('player-matches-count');
    if (!tableBody || !cardsContainer || !emptyState) return;

    const matches = data?.matches || [];
    if (!append) {
      tableBody.innerHTML = '';
      cardsContainer.innerHTML = '';
      emptyState.classList.add('hidden');
    }

    if (!matches.length && !append) {
      emptyState.classList.remove('hidden');
      if (countEl) countEl.textContent = '0 matches';
      return;
    }

    emptyState.classList.add('hidden');
    if (countEl) {
      const total = Number(data?.total || 0);
      countEl.textContent = `${total} match${total === 1 ? '' : 'es'}`;
    }

    let tableHtml = data?.table_rows_html || '';
    let cardHtml = data?.cards_html || '';
    if (matches.length && (!tableHtml.trim() || !cardHtml.trim())) {
      if (!tableHtml.trim()) tableHtml = buildFallbackTableRows(matches);
      if (!cardHtml.trim()) cardHtml = buildFallbackCards(matches);
    }

    if (append) {
      tableBody.insertAdjacentHTML('beforeend', tableHtml);
      cardsContainer.insertAdjacentHTML('beforeend', cardHtml);
    } else {
      tableBody.innerHTML = tableHtml;
      cardsContainer.innerHTML = cardHtml;
    }

    window.lucide?.createIcons?.();
  }

  async function loadOverviewKpis() {
    setSectionLoading('kpi-loading', ['overview-kpis'], true);
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/profile-overview`);
      const p = data.player || {};
      const o = data.overall_elo || {};
      setText('player-name', p.name);
      const meta = [];
      if (p.nationality) meta.push(p.nationality);
      if (p.age_years != null) meta.push(`${p.age_years}y`);
      if (p.hand) meta.push(`${p.hand}-handed`);
      setText('player-meta', meta.join(' • ') || 'No bio data available');
      setText('kpi-elo', o.rating);
      setText('kpi-rank', o.rank ? `#${o.rank}` : null);
      setText('kpi-peak', o.career_peak);
    } catch (e) {
      console.error(e);
      setText('player-meta', 'Failed to load player overview');
    } finally {
      setSectionLoading('kpi-loading', ['overview-kpis'], false);
    }
  }

  async function loadHistory() {
    setSectionLoading('history-loading', ['history-content'], true);
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/elo-history?range=${encodeURIComponent(state.range)}&surface=${encodeURIComponent(state.surface)}`);
      renderHistoryChart(data);
    } catch (e) {
      console.error(e);
      renderHistoryChart({ points: [] });
    } finally {
      setSectionLoading('history-loading', ['history-content'], false);
    }
  }

  async function loadSurfaceElo() {
    setSectionLoading('surface-loading', ['surface-cards'], true);
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/surface-elo`);
      renderSurfaceCards(data.surface_elo || []);
      setText(
        'surface-updated-at',
        data.generated_at ? `Updated: ${data.generated_at.replace('T', ' ').slice(0, 19)} UTC` : null
      );
    } catch (e) {
      console.error(e);
      renderSurfaceCards([]);
      setText('surface-updated-at', 'Failed to load');
    } finally {
      setSectionLoading('surface-loading', ['surface-cards'], false);
    }
  }

  async function loadRecords() {
    setSectionLoading('records-loading', ['record-cards'], true);
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/records`);
      const records = data.records || {};
      renderRecord('record-career', records.career);
      renderRecord('record-52w', records.last_52_weeks);
      renderRecord('record-last10', records.last_10);
      const last10 = records.last_10 || null;
      setText('kpi-last10', last10 ? `${last10.wins}-${last10.losses}` : null);
    } catch (e) {
      console.error(e);
      renderRecord('record-career', null);
      renderRecord('record-52w', null);
      renderRecord('record-last10', null);
      setText('kpi-last10', null);
    } finally {
      setSectionLoading('records-loading', ['record-cards'], false);
    }
  }

  async function loadMatches(append = false) {
    if (state.matches.loading) return;
    state.matches.loading = true;
    if (!append) setSectionLoading('matches-loading', ['matches-content'], true);

    const resultValue = byId('matches-result')?.value || 'all';
    const surfaceValue = byId('matches-surface')?.value || '';
    const page = append ? state.matches.page : 1;
    let url = `/api/players/${encodeURIComponent(playerId)}/matches?page=${encodeURIComponent(page)}&per_page=10&result=${encodeURIComponent(resultValue)}`;
    if (surfaceValue) url += `&surface=${encodeURIComponent(surfaceValue)}`;

    try {
      const data = await getJson(url);
      renderMatches(data, append);
      state.matches.page = (data.page || 1) + 1;
      state.matches.hasMore = !!data.has_more;
      toggleHidden(byId('player-matches-more'), !state.matches.hasMore);
    } catch (e) {
      console.error(e);
      if (!append) renderMatches({ matches: [], total: 0, table_rows_html: '', cards_html: '' }, false);
    } finally {
      state.matches.loading = false;
      if (!append) setSectionLoading('matches-loading', ['matches-content'], false);
    }
  }

  async function loadTournaments() {
    setSectionLoading('tournaments-loading', ['tournaments-content'], true);
    let allTournaments = [];
    let resultFilter = 'all';
    let surfaceFilter = 'all';
    let tournamentPage = 1;
    const TOURNAMENTS_PER_PAGE = 10;

    const renderTournamentSummary = (summary) => {
      const el = byId('tournament-summary');
      if (!el) return;
      const chips = [
        { label: 'Titles', value: summary.titles || 0, bg: '#ccff00', color: '#1a1a1a' },
        { label: 'Finals', value: summary.finals || 0, bg: '#fef08a', color: '#713f12' },
        { label: 'Semis', value: summary.semifinals || 0, bg: '#bfdbfe', color: '#1e3a5f' },
        { label: 'Quarters', value: summary.quarterfinals || 0, bg: '#fed7aa', color: '#7c2d12' },
      ];
      el.innerHTML = chips
        .map(
          (c) => `<span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold" style="background:${c.bg};color:${c.color}">
            ${escapeHtml(c.label)} <span class="font-bold">${c.value}</span>
          </span>`
        )
        .join('');
    };

    const getResultBadge = (result, wonTitle) => {
      const label = wonTitle ? 'W' : result;
      const styles = {
        W: 'background:#ccff00;color:#1a1a1a',
        F: 'background:#fef08a;color:#713f12',
        SF: 'background:#bfdbfe;color:#1e3a5f',
        QF: 'background:#fed7aa;color:#7c2d12',
        RR: 'background:#e9d5ff;color:#581c87',
      };
      const style = styles[label] || 'background:#f3f4f6;color:#6b7280';
      return `<span class="inline-block px-2 py-0.5 rounded text-xs font-bold" style="${style}">${escapeHtml(label || '')}</span>`;
    };

    const getSurfaceDot = (surface) => {
      const colors = { Hard: '#3b82f6', Clay: '#ea580c', Grass: '#16a34a' };
      const color = colors[surface] || '#6b7280';
      return `<span class="inline-block w-2 h-2 rounded-full mr-1" style="background:${color}"></span>`;
    };

    const renderTournamentTable = (tournaments) => {
      const body = byId('player-tournaments-body');
      const cards = byId('player-tournaments-cards');

      if (body) {
        if (!tournaments.length) {
          body.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-400">No results found.</td></tr>';
        } else {
          body.innerHTML = tournaments
            .map((t) => {
              const oppPrefix = t.opponent_name ? (t.won_title ? 'def. ' : 'lost to ') : '';
              const opponent = t.opponent_name ? `${oppPrefix}${escapeHtml(t.opponent_name)}` : '—';
              const surface = t.surface || '—';
              return `
                <tr class="border-b border-gray-100 hover:bg-gray-50/50">
                  <td class="py-2 pr-4 tabular-nums text-gray-600">${t.year || '—'}</td>
                  <td class="py-2 pr-4 font-medium text-gray-800">${escapeHtml(t.tournament_name || '—')}</td>
                  <td class="py-2 pr-4 text-gray-500 text-xs">${escapeHtml(t.tournament_level || '—')}</td>
                  <td class="py-2 pr-4 text-gray-600 text-xs">${getSurfaceDot(surface)}${escapeHtml(surface)}</td>
                  <td class="py-2 pr-4">${getResultBadge(t.result, t.won_title)}</td>
                  <td class="py-2 text-gray-500 text-xs">${opponent}</td>
                </tr>
              `;
            })
            .join('');
        }
      }

      if (cards) {
        if (!tournaments.length) {
          cards.innerHTML = '<div class="py-8 text-center text-gray-400">No results found.</div>';
        } else {
          cards.innerHTML = tournaments
            .map((t) => {
              const oppPrefix = t.opponent_name ? (t.won_title ? 'def. ' : 'lost to ') : '';
              const opponent = t.opponent_name ? `${oppPrefix}${escapeHtml(t.opponent_name)}` : '—';
              const surface = t.surface || '—';
              return `
                <div class="py-3">
                  <div class="flex items-center justify-between mb-1">
                    <span class="font-semibold text-gray-800 text-sm">${escapeHtml(t.tournament_name || '—')}</span>
                    ${getResultBadge(t.result, t.won_title)}
                  </div>
                  <div class="text-xs text-gray-500 mb-0.5">${t.year || '—'} · ${escapeHtml(t.tournament_level || '—')} · ${getSurfaceDot(surface)}${escapeHtml(surface)}</div>
                  <div class="text-xs text-gray-400">${opponent}</div>
                </div>
              `;
            })
            .join('');
        }
      }
    };

    const applyTournamentFilters = () => {
      const filtered = allTournaments.filter((t) => {
        let resultMatch = true;
        if (resultFilter === 'title') resultMatch = !!t.won_title;
        else if (resultFilter === 'final') resultMatch = t.result === 'F' && !t.won_title;
        else if (resultFilter === 'semi') resultMatch = t.result === 'SF';
        else if (resultFilter === 'quarter') resultMatch = t.result === 'QF';
        const surfaceMatch = surfaceFilter === 'all' || t.surface === surfaceFilter;
        return resultMatch && surfaceMatch;
      });

      const visible = filtered.slice(0, tournamentPage * TOURNAMENTS_PER_PAGE);
      renderTournamentTable(visible);
      toggleHidden(byId('tournaments-load-more'), visible.length >= filtered.length);
    };

    const setupTournamentFilters = () => {
      queryAll('[data-tournament-result-filter]').forEach((btn) => {
        btn.addEventListener('click', () => {
          queryAll('[data-tournament-result-filter]').forEach((b) => b.classList.remove('active'));
          btn.classList.add('active');
          resultFilter = btn.getAttribute('data-tournament-result-filter') || 'all';
          tournamentPage = 1;
          applyTournamentFilters();
        });
      });

      queryAll('[data-tournament-surface-filter]').forEach((btn) => {
        btn.addEventListener('click', () => {
          queryAll('[data-tournament-surface-filter]').forEach((b) => b.classList.remove('active'));
          btn.classList.add('active');
          surfaceFilter = btn.getAttribute('data-tournament-surface-filter') || 'all';
          tournamentPage = 1;
          applyTournamentFilters();
        });
      });

      byId('tournaments-load-more-btn')?.addEventListener('click', () => {
        tournamentPage += 1;
        applyTournamentFilters();
      });
    };

    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/tournaments`);
      allTournaments = data.tournaments || [];
      renderTournamentSummary(data.summary || {});
      applyTournamentFilters();
      setupTournamentFilters();
    } catch (e) {
      console.error(e);
      const body = byId('player-tournaments-body');
      const cards = byId('player-tournaments-cards');
      if (body) body.innerHTML = '<tr><td colspan="6" class="py-4 text-gray-400">Failed to load tournament data.</td></tr>';
      if (cards) cards.innerHTML = '<div class="py-4 text-center text-gray-400">Failed to load tournament data.</div>';
    } finally {
      setSectionLoading('tournaments-loading', ['tournaments-content'], false);
    }
  }

  queryAll('.history-range-btn', root).forEach((btn) => {
    btn.addEventListener('click', () => {
      state.range = btn.dataset.range || 'career';
      queryAll('.history-range-btn', root).forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      loadHistory();
    });
  });
  byId('history-surface')?.addEventListener('change', (e) => {
    state.surface = e.target.value || 'overall';
    loadHistory();
  });
  byId('matches-result')?.addEventListener('change', () => {
    state.matches.page = 1;
    loadMatches(false);
  });
  byId('matches-surface')?.addEventListener('change', () => {
    state.matches.page = 1;
    loadMatches(false);
  });
  byId('player-matches-more')?.addEventListener('click', () => loadMatches(true));

  loadOverviewKpis();
  loadSurfaceElo();
  loadRecords();
  loadHistory();
  loadMatches(false);
  loadTournaments();
  window.lucide?.createIcons?.();
}
