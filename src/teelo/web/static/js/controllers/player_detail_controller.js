import { byId } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { escapeHtml } from '../lib/format.js';

export function initPlayerDetailPage() {
  const root = byId('player-page-root');
  if (!root) return;
  const playerId = root.dataset.playerId;
  if (!playerId) return;

  const state = { range: 'career', surface: 'overall', matchesPage: 1, matchesLoading: false, matchesHasMore: false };

  function setText(id, value) {
    const el = byId(id);
    if (el) el.textContent = value == null || value === '' ? '—' : String(value);
  }

  async function loadOverview() {
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/profile-overview`);
      const p = data.player || {}; const o = data.overall_elo || {};
      setText('player-name', p.name);
      setText('player-meta', [p.nationality, p.age_years != null ? `${p.age_years}y` : null].filter(Boolean).join(' • '));
      setText('kpi-elo', o.rating); setText('kpi-rank', o.rank ? `#${o.rank}` : null); setText('kpi-peak', o.career_peak);
    } catch (e) { console.error(e); }
  }

  async function loadHistory() {
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/elo-history?range=${encodeURIComponent(state.range)}&surface=${encodeURIComponent(state.surface)}`);
      setText('history-meta', `Showing ${data.returned_points || 0} / ${data.total_points || 0} points`);
    } catch (e) { console.error(e); setText('history-meta', 'No chart data'); }
  }

  function renderMatches(data, append) {
    const table = byId('player-matches-table-body');
    const cards = byId('player-matches-cards-container');
    const empty = byId('player-matches-empty');
    if (!table || !cards || !empty) return;
    const matches = data.matches || [];
    if (!append) { table.innerHTML = ''; cards.innerHTML = ''; }
    if (!matches.length && !append) { empty.classList.remove('hidden'); return; }
    empty.classList.add('hidden');
    const fallback = matches.map((m) => `<tr><td>${escapeHtml(m.tournament_name || 'Unknown')}</td><td>${escapeHtml(m.score || '')}</td></tr>`).join('');
    if (append) table.insertAdjacentHTML('beforeend', data.table_rows_html || fallback); else table.innerHTML = data.table_rows_html || fallback;
  }

  async function loadMatches(append = false) {
    if (state.matchesLoading) return;
    state.matchesLoading = true;
    try {
      const data = await getJson(`/api/players/${encodeURIComponent(playerId)}/matches?page=${state.matchesPage}&per_page=10`);
      renderMatches(data, append);
      state.matchesPage = (data.page || 1) + 1;
      state.matchesHasMore = !!data.has_more;
      byId('player-matches-more')?.classList.toggle('hidden', !state.matchesHasMore);
    } catch (e) { console.error(e); }
    state.matchesLoading = false;
  }

  root.querySelectorAll('.history-range-btn').forEach((btn) => btn.addEventListener('click', () => { state.range = btn.dataset.range || 'career'; loadHistory(); }));
  byId('history-surface')?.addEventListener('change', (e) => { state.surface = e.target.value || 'overall'; loadHistory(); });
  byId('player-matches-more')?.addEventListener('click', () => loadMatches(true));

  loadOverview();
  loadHistory();
  loadMatches(false);
}
