import { byId, toggleHidden } from './lib/dom.js';
import { getJson } from './lib/http.js';
import { buildFallbackCards, buildFallbackTableRows } from './renderers/matches.js';

function normalizeSectionData(data, key) {
  const section = data?.[key];
  const topLevelTableHtml = data?.[`${key}_table_html`] || '';
  const topLevelCardsHtml = data?.[`${key}_cards_html`] || '';

  if (Array.isArray(section)) {
    return {
      matches: section,
      table_rows_html: topLevelTableHtml,
      cards_html: topLevelCardsHtml,
    };
  }

  if (section && typeof section === 'object') {
    return {
      matches: Array.isArray(section.matches) ? section.matches : [],
      table_rows_html: section.table_rows_html || topLevelTableHtml,
      cards_html: section.cards_html || topLevelCardsHtml,
    };
  }

  return {
    matches: [],
    table_rows_html: topLevelTableHtml,
    cards_html: topLevelCardsHtml,
  };
}

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

  try {
    const data = await getJson('/api/home');
    updateStats(data?.stats || {});

    renderSection('upcoming', normalizeSectionData(data, 'upcoming'));
    renderSection('completed', normalizeSectionData(data, 'completed'));
  } catch {
    renderSection('upcoming', { matches: [] });
    renderSection('completed', { matches: [] });
  } finally {
    window.lucide?.createIcons?.();
  }
}

document.addEventListener('DOMContentLoaded', initHomePage);
