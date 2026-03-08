import { byId, queryAll, setSectionLoading, toggleHidden } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { escapeHtml, slugifyName } from '../lib/format.js';
import { buildFallbackCards, buildFallbackTableRows } from '../renderers/matches.js';
import { renderBracket } from '../renderers/bracket.js';

const TAB_ACTIVE_CLASSES = ['border-b-2', 'border-teelo-lime', 'text-teelo-dark', 'font-semibold'];
const TAB_INACTIVE_CLASSES = ['border-b-2', 'border-transparent', 'text-gray-500', 'hover:text-gray-700'];

function surfaceClass(surface) {
  if (surface === 'Hard') return 'surface-text-hard';
  if (surface === 'Clay') return 'surface-text-clay';
  if (surface === 'Grass') return 'surface-text-grass';
  if (surface === 'Indoor') return 'surface-text-indoor';
  return '';
}

function renderPlayerLink(name, playerId, className = '', playerUrl = null) {
  const safeName = escapeHtml(name || '—');
  if (Number.isFinite(Number(playerId))) {
    const fallback = `/players/${encodeURIComponent(String(playerId))}/${slugifyName(name)}`;
    return `<a href="${escapeHtml(playerUrl || fallback)}" class="${className} hover:underline decoration-teelo-lime decoration-2">${safeName}</a>`;
  }
  return `<span class="${className}">${safeName}</span>`;
}

export function initTournamentDetailPage() {
  const root = byId('tournament-page-root');
  if (!root) return;

  const tour = root.dataset.tour || '';
  const tournamentCode = root.dataset.tournamentCode || '';
  const year = root.dataset.year || '';
  const hasDraw = root.dataset.hasDraw === 'true';
  if (!tour || !tournamentCode || !year) return;

  const state = {
    activeTab: 'matches',
    matchStatus: 'completed',
    matchPage: 1,
    drawLoaded: false,
    editionsLoaded: false,
    matchesLoading: false,
  };

  function setTabClasses(button, active) {
    if (!button) return;
    TAB_ACTIVE_CLASSES.forEach((cls) => button.classList.toggle(cls, active));
    TAB_INACTIVE_CLASSES.forEach((cls) => button.classList.toggle(cls, !active));
  }

  function setActiveTab(tab) {
    const normalized = tab === 'draw' && hasDraw ? 'draw' : 'matches';
    state.activeTab = normalized;

    queryAll('[data-tab]', root).forEach((button) => {
      setTabClasses(button, button.dataset.tab === normalized);
    });

    toggleHidden(byId('tab-panel-matches'), normalized !== 'matches');
    if (hasDraw) {
      toggleHidden(byId('tab-panel-draw'), normalized !== 'draw');
    }

    if (normalized === 'draw' && hasDraw && !state.drawLoaded) {
      loadDraw();
    }
  }

  function renderMatches(data) {
    const tableBody = byId('tournament-matches-table-body');
    const cardsContainer = byId('tournament-matches-cards-container');
    const emptyState = byId('tournament-matches-empty');
    const pagination = byId('matches-pagination');
    if (!tableBody || !cardsContainer || !emptyState || !pagination) return;

    const matches = data?.matches || [];
    const total = Number(data?.total) || 0;
    const perPage = Number(data?.per_page) || 10;
    const totalPages = Math.max(1, Math.ceil(total / perPage));
    const hasMore = Boolean(data?.has_more);
    const showPagination = hasMore || state.matchPage > 1;
    if (!matches.length) {
      tableBody.innerHTML = '';
      cardsContainer.innerHTML = '';
      emptyState.classList.remove('hidden');
    } else {
      emptyState.classList.add('hidden');
      let tableHtml = data?.table_rows_html || '';
      let cardHtml = data?.cards_html || '';
      if (!tableHtml.trim()) tableHtml = buildFallbackTableRows(matches);
      if (!cardHtml.trim()) cardHtml = buildFallbackCards(matches);

      tableBody.innerHTML = tableHtml;
      cardsContainer.innerHTML = cardHtml;
    }

    if (!showPagination) {
      pagination.classList.add('hidden');
      pagination.innerHTML = '';
      window.lucide?.createIcons?.();
      return;
    }

    pagination.className = 'flex items-center justify-center gap-4 pt-4';
    pagination.innerHTML = `
      <button
        type="button"
        data-pagination="previous"
        class="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
        ${state.matchPage <= 1 ? 'disabled' : ''}
      >
        Previous
      </button>
      <span class="text-sm text-gray-500">Page ${state.matchPage} of ${totalPages}</span>
      <button
        type="button"
        data-pagination="next"
        class="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
        ${!hasMore ? 'disabled' : ''}
      >
        Next
      </button>
    `;

    pagination.querySelector('[data-pagination="previous"]')?.addEventListener('click', () => {
      if (state.matchPage <= 1) return;
      state.matchPage -= 1;
      loadMatches();
    });

    pagination.querySelector('[data-pagination="next"]')?.addEventListener('click', () => {
      if (!hasMore) return;
      state.matchPage += 1;
      loadMatches();
    });

    window.lucide?.createIcons?.();
  }

  async function loadMatches() {
    if (state.matchesLoading) return;
    state.matchesLoading = true;
    const matchesContent = byId('matches-content');
    const matchesLoading = byId('matches-loading');
    const currentHeight = matchesContent ? matchesContent.getBoundingClientRect().height : 0;

    if (matchesContent && currentHeight > 0) {
      matchesContent.style.minHeight = `${Math.ceil(currentHeight)}px`;
    }
    if (matchesContent) {
      matchesContent.classList.remove('hidden');
      matchesContent.classList.add('opacity-60', 'pointer-events-none', 'transition-opacity', 'duration-150');
    }
    matchesLoading?.classList.add('hidden');

    const qs = new URLSearchParams({
      status: state.matchStatus,
      page: String(state.matchPage),
      per_page: '10',
    });

    try {
      const data = await getJson(
        `/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(year)}/matches?${qs.toString()}`
      );
      renderMatches(data);
    } catch (error) {
      console.error(error);
      renderMatches({ matches: [] });
    } finally {
      state.matchesLoading = false;
      if (matchesContent) {
        matchesContent.style.minHeight = '';
        matchesContent.classList.remove('opacity-60', 'pointer-events-none');
      }
      matchesLoading?.classList.add('hidden');
    }
  }

  async function loadDraw() {
    setSectionLoading('draw-loading', ['draw-content'], true);
    const bracketContainer = byId('bracket-container');
    try {
      const data = await getJson(
        `/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(year)}/draw`
      );
      if (bracketContainer) {
        renderBracket(bracketContainer, data);
      }
      state.drawLoaded = true;
      window.lucide?.createIcons?.();
    } catch (error) {
      console.error(error);
      if (bracketContainer) {
        bracketContainer.innerHTML = '<p class="text-sm text-gray-400 py-6">Failed to load draw.</p>';
      }
    } finally {
      setSectionLoading('draw-loading', ['draw-content'], false);
    }
  }

  function renderEditionHistory(editions) {
    const tableBody = byId('edition-history-table-body');
    const cards = byId('edition-history-cards');
    const empty = byId('edition-history-empty');
    if (!tableBody || !cards || !empty) return;

    if (!editions.length) {
      tableBody.innerHTML = '';
      cards.innerHTML = '';
      empty.classList.remove('hidden');
      return;
    }

    empty.classList.add('hidden');
    const currentYear = Number.parseInt(year, 10);

    tableBody.innerHTML = editions
      .map((item) => {
        const isCurrent = Number(item.year) === currentYear;
        const rowClass = isCurrent ? 'bg-teelo-lime/20' : 'hover:bg-gray-50/50';
        const yearCellClass = isCurrent ? 'text-teelo-dark font-bold' : 'text-gray-600';
        const champion = item.champion || '—';
        const runnerUp = item.runner_up || '—';
        const score = item.score || '—';
        const surface = item.surface || '—';
        const championCell = renderPlayerLink(champion, item.champion_id, 'text-teelo-dark font-semibold', item.champion_url || null);
        const runnerUpCell = renderPlayerLink(runnerUp, item.runner_up_id, 'text-gray-600', item.runner_up_url || null);

        return `
          <tr class="${rowClass}">
            <td class="px-5 py-3 ${yearCellClass}"><a href="${escapeHtml(item.url || '#')}" class="hover:underline decoration-teelo-lime decoration-2">${escapeHtml(item.year)}</a></td>
            <td class="px-5 py-3">${championCell}</td>
            <td class="px-5 py-3">${runnerUpCell}</td>
            <td class="px-5 py-3 text-gray-600 font-mono text-xs">${escapeHtml(score)}</td>
            <td class="px-5 py-3 text-sm ${surfaceClass(surface)}">${escapeHtml(surface)}</td>
          </tr>
        `;
      })
      .join('');

    cards.innerHTML = editions
      .map((item) => {
        const isCurrent = Number(item.year) === currentYear;
        const cardClass = isCurrent ? 'bg-teelo-lime/20' : 'bg-white';
        const surface = item.surface || '—';
        const championCell = renderPlayerLink(item.champion || '—', item.champion_id, 'font-semibold text-teelo-dark', item.champion_url || null);
        const runnerUpCell = renderPlayerLink(item.runner_up || '—', item.runner_up_id, '', item.runner_up_url || null);
        return `
          <a href="${escapeHtml(item.url || '#')}" class="block px-4 py-3 ${cardClass}">
            <div class="flex items-center justify-between gap-2 mb-1.5">
              <span class="text-sm font-bold text-teelo-dark">${escapeHtml(item.year)}</span>
              <span class="text-[11px] ${surfaceClass(surface)} font-semibold">${escapeHtml(surface)}</span>
            </div>
            <div class="text-xs text-gray-600">
              ${championCell}
              <span class="text-gray-400"> def. </span>
              ${runnerUpCell}
            </div>
            <div class="text-[11px] font-mono text-gray-500 mt-1">${escapeHtml(item.score || '—')}</div>
          </a>
        `;
      })
      .join('');
  }

  async function loadEditions() {
    if (state.editionsLoaded) return;
    setSectionLoading('editions-loading', ['editions-content'], true);
    try {
      const data = await getJson(`/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/editions`);
      renderEditionHistory(data?.editions || []);
      state.editionsLoaded = true;
    } catch (error) {
      console.error(error);
      renderEditionHistory([]);
    } finally {
      setSectionLoading('editions-loading', ['editions-content'], false);
    }
  }

  queryAll('[data-tab]', root).forEach((button) => {
    button.addEventListener('click', () => {
      setActiveTab(button.dataset.tab || 'matches');
    });
  });

  queryAll('[data-match-status]', root).forEach((button) => {
    button.addEventListener('click', () => {
      const nextStatus = button.dataset.matchStatus || 'completed';
      if (state.matchStatus === nextStatus) return;
      state.matchStatus = nextStatus;
      state.matchPage = 1;
      queryAll('[data-match-status]', root).forEach((chip) => {
        chip.classList.toggle('active', chip === button);
      });
      loadMatches();
    });
  });

  const yearSelect = byId('tournament-year-select');
  yearSelect?.addEventListener('change', (event) => {
    const nextYear = event.target?.value;
    if (!nextYear || nextYear === year) return;
    window.location.assign(`/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(nextYear)}`);
  });

  setActiveTab('matches');
  loadMatches();
  loadEditions();
  window.lucide?.createIcons?.();
}
