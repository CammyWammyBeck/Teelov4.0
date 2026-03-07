import { byId, toggleHidden } from './lib/dom.js';
import { getJson } from './lib/http.js';
import { buildFallbackCards, buildFallbackTableRows } from './renderers/matches.js';

function statValue(value) {
  const numeric = Number(value ?? 0);
  return Number.isFinite(numeric) ? numeric.toLocaleString() : '0';
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

  if (matchesEl) matchesEl.textContent = statValue(stats?.matches_total);
  if (playersEl) playersEl.textContent = statValue(stats?.players_total);
  if (editionsEl) editionsEl.textContent = statValue(stats?.editions_total);
}

export async function initHomePage() {
  if (!byId('home-stats-section')) return;

  // Fire all 3 requests in parallel, render each as it resolves
  const upcomingPromise = getJson('/api/home/upcoming')
    .then(data => {
      renderSection('upcoming', {
        matches: data?.matches || [],
        table_rows_html: data?.table_html || '',
        cards_html: data?.cards_html || '',
      });
    })
    .catch(() => renderSection('upcoming', { matches: [] }));

  const completedPromise = getJson('/api/home/completed')
    .then(data => {
      renderSection('completed', {
        matches: data?.matches || [],
        table_rows_html: data?.table_html || '',
        cards_html: data?.cards_html || '',
      });
    })
    .catch(() => renderSection('completed', { matches: [] }));

  const statsPromise = getJson('/api/home/stats')
    .then(stats => updateStats(stats || {}))
    .catch(() => updateStats({}));

  await Promise.allSettled([upcomingPromise, completedPromise, statsPromise]);
  window.lucide?.createIcons?.();
}

document.addEventListener('DOMContentLoaded', initHomePage);
