import { byId, toggleHidden } from './lib/dom.js';
import { getJson } from './lib/http.js';
import { escapeHtml, formatShortDate } from './lib/format.js';

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

function fallbackTableRows(matches) {
  return matches
    .map((match) => {
      const tournament = escapeHtml(match?.tournament_name || 'Unknown');
      const round = escapeHtml(match?.round || '');
      const playerA = escapeHtml(match?.player_a?.name || 'Unknown');
      const playerB = escapeHtml(match?.player_b?.name || 'Unknown');
      const score = escapeHtml(match?.status === 'walkover' ? 'W/O' : match?.score || 'vs');
      const dateText = escapeHtml(match?.match_date_display || formatShortDate(match?.match_date));

      return `
<tr class="hover:bg-gray-50/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime">
  <td class="px-5 py-3"><div class="min-w-0"><span class="text-sm font-semibold text-teelo-dark truncate block">${tournament}</span><span class="text-xs text-gray-400">${round}</span></div></td>
  <td class="px-5 py-3 text-right"><span class="text-sm text-teelo-dark">${playerA}</span></td>
  <td class="px-5 py-3 text-center"><span class="inline-block px-2.5 py-1 bg-gray-50 rounded-md text-xs font-mono whitespace-nowrap text-teelo-dark font-semibold">${score}</span></td>
  <td class="px-5 py-3"><span class="text-sm text-teelo-dark">${playerB}</span></td>
  <td class="px-5 py-3 text-right"><span class="text-xs text-gray-400 whitespace-nowrap">${dateText}</span></td>
</tr>`;
    })
    .join('');
}

function fallbackCards(matches) {
  return matches
    .map((match) => {
      const tournament = escapeHtml(match?.tournament_name || 'Unknown');
      const round = escapeHtml(match?.round || '');
      const playerA = escapeHtml(match?.player_a?.name || 'Unknown');
      const playerB = escapeHtml(match?.player_b?.name || 'Unknown');
      const score = escapeHtml(match?.status === 'walkover' ? 'W/O' : match?.score || 'vs');
      const dateText = escapeHtml(match?.match_date_display || formatShortDate(match?.match_date));

      return `
<div class="px-4 py-3 border-b border-gray-100 last:border-b-0">
  <div class="text-[13px] font-semibold text-teelo-dark truncate">${tournament}</div>
  <div class="text-[11px] text-gray-400 mb-2">${round} ${dateText ? `· ${dateText}` : ''}</div>
  <div class="flex items-center justify-between gap-2">
    <div class="min-w-0 flex-1">
      <div class="text-[13px] text-teelo-dark truncate">${playerA}</div>
      <div class="text-[13px] text-teelo-dark truncate">${playerB}</div>
    </div>
    <span class="text-xs font-mono whitespace-nowrap text-teelo-dark font-semibold">${score}</span>
  </div>
</div>`;
    })
    .join('');
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
      ? (tableRowsHtml || fallbackTableRows(matches))
      : '';
  }
  if (cardsEl) {
    cardsEl.innerHTML = hasMatches
      ? (cardsHtml || fallbackCards(matches))
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
    if (window.lucide?.createIcons) {
      window.lucide.createIcons();
    }
  }
}

document.addEventListener('DOMContentLoaded', initHomePage);
