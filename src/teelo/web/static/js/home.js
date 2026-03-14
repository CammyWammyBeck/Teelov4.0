import { byId, toggleHidden } from './lib/dom.js';
import { getJson } from './lib/http.js';
import { buildFallbackCards, buildFallbackTableRows } from './renderers/matches.js';

function statValue(value) {
  const numeric = Number(value ?? 0);
  return Number.isFinite(numeric) ? numeric.toLocaleString() : '0';
}

function fmtDate(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
}

const SURFACE_COLORS = {
  Hard: 'bg-blue-100 text-blue-700',
  Clay: 'bg-orange-100 text-orange-700',
  Grass: 'bg-status-success-subtle text-status-success',
  Carpet: 'bg-purple-100 text-purple-700',
};

function surfaceBadge(surface) {
  if (!surface) return '';
  const cls = SURFACE_COLORS[surface] || 'bg-surface-muted text-content-secondary';
  return `<span class="text-[10px] font-semibold px-1.5 py-0.5 rounded ${cls}">${surface}</span>`;
}

function renderSection(prefix, sectionData) {
  const loadingEl = byId(`${prefix}-loading`);
  const contentEl = byId(`${prefix}-content`);
  const emptyEl = byId(`${prefix}-empty`);
  const tableBodyEl = byId(`${prefix}-table-body`);
  const cardsEl = byId(`${prefix}-cards`);

  const matches = Array.isArray(sectionData.matches) ? sectionData.matches : [];
  const tableRowsHtml = (sectionData.table_rows_html || '').trim();
  const cardsHtml = (sectionData.cards_html || '').trim();
  const hasMatches = matches.length > 0 || tableRowsHtml.length > 0 || cardsHtml.length > 0;

  if (tableBodyEl) {
    tableBodyEl.innerHTML = hasMatches
      ? (tableRowsHtml || buildFallbackTableRows(matches))
      : '';
  }
  if (cardsEl) {
    cardsEl.innerHTML = hasMatches
      ? (cardsHtml || buildFallbackCards(matches))
      : '';
  }

  toggleHidden(loadingEl, true);
  toggleHidden(contentEl, !hasMatches);
  toggleHidden(emptyEl, hasMatches);
}

function updateStats(stats) {
  const matchesEl = byId('stat-matches');
  const playersEl = byId('stat-players');
  const editionsEl = byId('stat-editions');
  const countriesEl = byId('stat-countries');
  const accuracyEl = byId('stat-accuracy');
  const daysEl = byId('stat-days');

  if (matchesEl) matchesEl.textContent = statValue(stats?.matches_total);
  if (playersEl) playersEl.textContent = statValue(stats?.players_total);
  if (editionsEl) editionsEl.textContent = statValue(stats?.editions_total);
  if (countriesEl) countriesEl.textContent = statValue(stats?.countries_total);
  if (accuracyEl) {
    const acc = stats?.prediction_accuracy_pct;
    accuracyEl.textContent = (acc != null) ? `${acc}%` : '—';
  }
  if (daysEl) daysEl.textContent = statValue(stats?.days_of_data);
}

function renderTournamentList(listEl, sectionEl, editions) {
  if (!editions || editions.length === 0) {
    toggleHidden(sectionEl, true);
    return;
  }
  toggleHidden(sectionEl, false);
  listEl.innerHTML = editions.map(t => {
    const href = t.url || '#';
    const dates = [t.start_date, t.end_date].filter(Boolean).map(fmtDate).join(' – ');
    const winnerHtml = t.winner_name
      ? `<span class="text-xs text-content-muted">Winner: <a href="${t.winner_url || '#'}" class="font-semibold text-teelo-dark hover:underline decoration-teelo-lime">${t.winner_name}</a></span>`
      : '';
    const badge = t.badge_label
      ? `<span class="inline-flex items-center justify-center text-[9px] font-bold text-content-inverse px-1.5 py-0.5 rounded ${t.badge_color} mr-1.5 leading-none">${t.badge_label}</span>`
      : '';
    return `
      <li class="px-5 py-3 flex items-center justify-between gap-2 hover:bg-surface-hover transition-colors">
        <div class="min-w-0">
          <div class="flex items-center">
            ${badge}<a href="${href}" class="text-sm font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 truncate">${t.tournament_name}</a>
          </div>
          <p class="text-xs text-content-faint mt-0.5">${t.city || ''} &middot; ${dates}</p>
          ${winnerHtml}
        </div>
        <div class="flex items-center gap-1.5 shrink-0">
          ${surfaceBadge(t.surface)}
        </div>
      </li>`;
  }).join('');
}

function renderTournaments(data) {
  const loadingEl = byId('tournaments-loading');
  const contentEl = byId('tournaments-content');
  const emptyEl = byId('tournaments-empty');

  const hasAny = (
    (data?.in_progress?.length || 0) +
    (data?.upcoming?.length || 0) +
    (data?.recently_completed?.length || 0)
  ) > 0;

  toggleHidden(loadingEl, true);

  if (!hasAny) {
    toggleHidden(emptyEl, false);
    return;
  }

  toggleHidden(contentEl, false);

  renderTournamentList(
    byId('tournaments-in-progress-list'),
    byId('tournaments-in-progress'),
    data?.in_progress,
  );
  renderTournamentList(
    byId('tournaments-upcoming-list'),
    byId('tournaments-upcoming'),
    data?.upcoming,
  );
  renderTournamentList(
    byId('tournaments-completed-list'),
    byId('tournaments-completed'),
    data?.recently_completed,
  );
}

function renderRankingsList(listId, loadingId, players) {
  const listEl = byId(listId);
  const loadingEl = byId(loadingId);
  toggleHidden(loadingEl, true);
  if (!listEl) return;
  if (!players || players.length === 0) {
    listEl.innerHTML = '<li class="px-5 py-4 text-sm text-content-faint">No data available.</li>';
  } else {
    listEl.innerHTML = players.map(p => `
      <li class="px-5 py-3 flex items-center gap-3">
        <span class="text-xs font-bold text-content-faintest w-5 text-right shrink-0">${p.rank}</span>
        <div class="flex-1 min-w-0">
          <a href="${p.url || '#'}" class="text-sm font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 truncate block">${p.name}</a>
          <span class="text-xs text-content-faint">${p.nationality_ioc || ''}</span>
        </div>
        <span class="text-sm font-bold text-teelo-dark shrink-0">${p.rating != null ? p.rating.toLocaleString() : '—'}</span>
      </li>`).join('');
  }
  toggleHidden(listEl, false);
}

function renderRankings(data) {
  renderRankingsList('rankings-atp-list', 'rankings-atp-loading', data?.atp);
  renderRankingsList('rankings-wta-list', 'rankings-wta-loading', data?.wta);
}

function renderMovers(data) {
  const loadingEl = byId('movers-loading');
  const listEl = byId('movers-list');
  const emptyEl = byId('movers-empty');

  toggleHidden(loadingEl, true);

  if (!data || !Array.isArray(data) || data.length === 0) {
    toggleHidden(emptyEl, false);
    return;
  }

  listEl.innerHTML = data.map(p => {
    const sign = p.rating_change >= 0 ? '+' : '';
    const changeColor = p.rating_change >= 0 ? 'text-status-success' : 'text-status-danger';
    const tourLabel = p.gender === 'women' ? 'WTA' : 'ATP';
    return `
      <li class="px-5 py-3 flex items-center gap-3">
        <div class="flex-1 min-w-0">
          <a href="${p.url || '#'}" class="text-sm font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 truncate block">${p.name}</a>
          <span class="text-xs text-content-faint">${p.nationality_ioc || ''} &middot; ${tourLabel} &middot; ${p.rating != null ? p.rating.toLocaleString() : '—'}</span>
        </div>
        <span class="text-sm font-bold ${changeColor} shrink-0">${sign}${p.rating_change}</span>
      </li>`;
  }).join('');
  toggleHidden(listEl, false);
}

function renderBlogPost(data) {
  const cardEl = byId('blog-post-card');
  if (!data || !cardEl) return;

  const dateEl = byId('blog-post-date');
  const linkEl = byId('blog-post-link');
  const excerptEl = byId('blog-post-excerpt');

  if (dateEl) dateEl.textContent = fmtDate(data.date);
  if (linkEl) {
    linkEl.href = data.url || '#';
    linkEl.textContent = data.title || 'Read post';
  }
  if (excerptEl) excerptEl.textContent = data.excerpt || '';

  toggleHidden(cardEl, false);
}

export async function initHomePage() {
  if (!byId('home-stats-section')) return;

  // Fire requests in visual top-to-bottom order so above-fold
  // sections get priority when browser connection slots are limited.
  const statsP = getJson('/api/home/stats')
    .then(stats => updateStats(stats || {}))
    .catch(() => updateStats({}));

  const rankingsP = getJson('/api/home/rankings')
    .then(data => renderRankings(data || {}))
    .catch(() => renderRankings({}));

  const tournamentsP = getJson('/api/home/tournaments')
    .then(data => renderTournaments(data || {}))
    .catch(() => renderTournaments({}));

  const upcomingP = getJson('/api/home/upcoming')
    .then(data => {
      renderSection('upcoming', {
        matches: data?.matches || [],
        table_rows_html: data?.table_html || '',
        cards_html: data?.cards_html || '',
      });
    })
    .catch(() => renderSection('upcoming', { matches: [] }));

  const completedP = getJson('/api/home/completed')
    .then(data => {
      renderSection('completed', {
        matches: data?.matches || [],
        table_rows_html: data?.table_html || '',
        cards_html: data?.cards_html || '',
      });
    })
    .catch(() => renderSection('completed', { matches: [] }));

  const moversP = getJson('/api/home/movers')
    .then(data => renderMovers(data))
    .catch(() => renderMovers([]));

  const blogP = getJson('/api/home/blog')
    .then(data => renderBlogPost(data))
    .catch(() => {});

  await Promise.allSettled([
    statsP, rankingsP, tournamentsP,
    upcomingP, completedP, moversP, blogP,
  ]);
  window.lucide?.createIcons?.();

  // Click-to-detail navigation for match rows
  document.addEventListener('click', (e) => {
    if (e.target.closest('a')) return;
    const row = e.target.closest('[data-match-url]');
    if (row) window.location.href = row.dataset.matchUrl;
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      const row = e.target.closest('[data-match-url]');
      if (row) { e.preventDefault(); window.location.href = row.dataset.matchUrl; }
    }
  });
}

document.addEventListener('DOMContentLoaded', initHomePage);
