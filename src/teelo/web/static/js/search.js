import { byId } from './lib/dom.js';
import { getJson } from './lib/http.js';
import { escapeHtml } from './lib/format.js';

const DEBOUNCE_MS = 250;

const TOUR_COLORS = {
  ATP: { bg: '#E8EEF9', label: '#002865' },
  WTA: { bg: '#FCE6F1', label: '#E30066' },
  CHALLENGER: { bg: '#E6F3EC', label: '#006B3F' },
  ITF: { bg: '#F1F3F5', label: '#4B5563' },
  'WTA 125': { bg: '#E6F3EC', label: '#006B3F' },
  WTA_125: { bg: '#E6F3EC', label: '#006B3F' },
};

function normalizeTour(tour) {
  return String(tour || '').trim().toUpperCase();
}

function tourStyle(tour) {
  return TOUR_COLORS[normalizeTour(tour)] || { bg: '#F1F3F5', label: '#374151' };
}

function buildBadge(label, tour) {
  const style = tourStyle(tour);
  return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold" style="background:${style.bg};color:${style.label};">${escapeHtml(label || '')}</span>`;
}

function renderResults(results, activeIndex) {
  const players = Array.isArray(results?.players) ? results.players : [];
  const tournaments = Array.isArray(results?.tournaments) ? results.tournaments : [];
  const playerCount = Number(results?.player_count) || players.length;
  const tournamentCount = Number(results?.tournament_count) || tournaments.length;
  const playerHasMore = Boolean(results?.player_has_more);
  const tournamentHasMore = Boolean(results?.tournament_has_more);
  const hasMore = playerHasMore || tournamentHasMore;

  if (!players.length && !tournaments.length) {
    return '<div class="px-4 py-4 text-sm text-gray-500">No results</div>';
  }

  let idx = -1;

  const playerRows = players.length
    ? players.map((player) => {
      idx += 1;
      const activeClass = idx === activeIndex ? 'bg-gray-50' : 'hover:bg-gray-50';
      return `
        <a
          href="${escapeHtml(player.url || '#')}"
          data-search-result-link
          class="flex items-center justify-between gap-3 px-4 py-2.5 ${activeClass} transition-colors"
        >
          <div class="min-w-0">
            <div class="text-sm font-medium text-teelo-dark truncate">${escapeHtml(player.name || 'Unknown Player')}</div>
            <div class="text-[11px] text-gray-400 mt-0.5">${escapeHtml(player.nationality_ioc || '')}</div>
          </div>
          ${buildBadge(player.tour || 'ATP', player.tour || 'ATP')}
        </a>
      `;
    }).join('')
    : '<div class="px-4 py-2.5 text-xs text-gray-400">No player matches</div>';

  const tournamentRows = tournaments.length
    ? tournaments.map((tournament) => {
      idx += 1;
      const activeClass = idx === activeIndex ? 'bg-gray-50' : 'hover:bg-gray-50';
      const locationParts = [tournament.city, tournament.country].filter((value) => String(value || '').trim());
      const metaParts = [tournament.level, locationParts.length ? locationParts.join(', ') : '']
        .filter((value) => String(value || '').trim());
      return `
        <a
          href="${escapeHtml(tournament.url || '#')}"
          data-search-result-link
          class="flex items-center justify-between gap-3 px-4 py-2.5 ${activeClass} transition-colors"
        >
          <div class="min-w-0">
            <div class="text-sm font-medium text-teelo-dark truncate">${escapeHtml(tournament.name || 'Unknown Tournament')}</div>
            <div class="text-[11px] text-gray-400 mt-0.5">${escapeHtml(metaParts.join(' · '))}</div>
          </div>
          ${buildBadge(tournament.tour || 'ITF', tournament.tour || 'ITF')}
        </a>
      `;
    }).join('')
    : '<div class="px-4 py-2.5 text-xs text-gray-400">No tournament matches</div>';

  return `
    <div class="max-h-96 overflow-y-auto">
      <div class="px-4 pt-3 pb-1 text-[11px] uppercase tracking-wide text-gray-400 font-semibold">Players (${playerCount}${playerHasMore ? '+' : ''})</div>
      ${playerRows}
      <div class="border-t border-gray-100"></div>
      <div class="px-4 pt-3 pb-1 text-[11px] uppercase tracking-wide text-gray-400 font-semibold">Tournaments (${tournamentCount}${tournamentHasMore ? '+' : ''})</div>
      ${tournamentRows}
      ${hasMore ? `<div class="border-t border-gray-100"></div><a href="/search?q=${encodeURIComponent(results?.query || '')}" class="block px-4 py-2.5 text-sm font-medium text-teelo-dark hover:bg-gray-50" data-search-result-link>See all results</a>` : ''}
    </div>
  `;
}

function setupSearchInstance({ root, input, results, onOpen, onClose, isVisible }) {
  if (!root || !input || !results) return null;

  const state = {
    query: '',
    timer: null,
    requestId: 0,
    activeIndex: -1,
    payload: { players: [], tournaments: [] },
  };

  function closeResults() {
    state.activeIndex = -1;
    results.classList.add('hidden');
    if (typeof onClose === 'function') onClose();
  }

  function openResults() {
    if (!state.query || state.query.length < 2) return;
    results.classList.remove('hidden');
    if (typeof onOpen === 'function') onOpen();
  }

  function render() {
    results.innerHTML = renderResults(state.payload, state.activeIndex);
    if (state.query.length >= 2) openResults();
    else closeResults();
  }

  async function runSearch(query, requestId) {
    try {
      const data = await getJson(`/api/search?q=${encodeURIComponent(query)}&limit=4`);
      if (requestId !== state.requestId) return;
      state.payload = {
        players: Array.isArray(data?.players) ? data.players : [],
        tournaments: Array.isArray(data?.tournaments) ? data.tournaments : [],
        player_count: Number(data?.player_count) || 0,
        player_has_more: Boolean(data?.player_has_more),
        tournament_count: Number(data?.tournament_count) || 0,
        tournament_has_more: Boolean(data?.tournament_has_more),
        query,
      };
      state.activeIndex = -1;
      render();
    } catch {
      if (requestId !== state.requestId) return;
      state.payload = { players: [], tournaments: [] };
      state.activeIndex = -1;
      render();
    }
  }

  function queueSearch() {
    window.clearTimeout(state.timer);
    if (state.query.length < 2) {
      state.payload = { players: [], tournaments: [] };
      render();
      return;
    }
    const requestId = ++state.requestId;
    state.timer = window.setTimeout(() => runSearch(state.query, requestId), DEBOUNCE_MS);
  }

  function links() {
    return Array.from(results.querySelectorAll('[data-search-result-link]'));
  }

  function moveSelection(delta) {
    const items = links();
    if (!items.length) return;
    if (results.classList.contains('hidden')) {
      openResults();
    }
    const total = items.length;
    const next = state.activeIndex + delta;
    state.activeIndex = ((next % total) + total) % total;
    render();
  }

  input.addEventListener('input', () => {
    state.query = (input.value || '').trim();
    queueSearch();
  });

  input.addEventListener('focus', () => {
    if (state.query.length >= 2) openResults();
  });

  input.addEventListener('keydown', (event) => {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      moveSelection(1);
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      moveSelection(-1);
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      const query = (input.value || '').trim();
      const items = links();
      const activeLink = state.activeIndex >= 0 ? items[state.activeIndex] : null;
      if (activeLink?.href) {
        window.location.href = activeLink.href;
        return;
      }
      if (query.length >= 2) {
        window.location.href = `/search?q=${encodeURIComponent(query)}`;
      }
    }
  });

  root.addEventListener('focusout', () => {
    window.setTimeout(() => {
      const visible = typeof isVisible === 'function' ? isVisible() : true;
      if (!visible) return;
      if (!root.contains(document.activeElement)) closeResults();
    }, 0);
  });

  return {
    closeResults,
    clear() {
      state.query = '';
      input.value = '';
      state.payload = { players: [], tournaments: [] };
      state.activeIndex = -1;
      window.clearTimeout(state.timer);
      render();
    },
    hasOpenResults() {
      return !results.classList.contains('hidden');
    },
    contains(node) {
      return root.contains(node);
    },
    focusInput() {
      input.focus();
    },
  };
}

function initGlobalSearch() {
  const desktop = setupSearchInstance({
    root: byId('desktop-search-root'),
    input: byId('desktop-search-input'),
    results: byId('desktop-search-results'),
    isVisible: () => window.matchMedia('(min-width: 768px)').matches,
  });

  const mobileOverlay = byId('mobile-search-overlay');
  const mobileOpenBtn = byId('mobile-search-btn');
  const mobileCloseBtn = byId('mobile-search-close');

  const mobile = setupSearchInstance({
    root: byId('mobile-search-root'),
    input: byId('mobile-search-input'),
    results: byId('mobile-search-results'),
    isVisible: () => mobileOverlay && !mobileOverlay.classList.contains('hidden'),
  });

  function openMobileOverlay() {
    if (!mobileOverlay) return;
    mobileOverlay.classList.remove('hidden');
    mobileOverlay.setAttribute('aria-hidden', 'false');
    mobile?.focusInput();
  }

  function closeMobileOverlay() {
    if (!mobileOverlay) return;
    mobileOverlay.classList.add('hidden');
    mobileOverlay.setAttribute('aria-hidden', 'true');
    mobile?.closeResults();
  }

  mobileOpenBtn?.addEventListener('click', openMobileOverlay);
  mobileCloseBtn?.addEventListener('click', closeMobileOverlay);

  document.addEventListener('mousedown', (event) => {
    const target = event.target;
    if (!(target instanceof Node)) return;

    if (desktop && desktop.hasOpenResults() && !desktop.contains(target)) {
      desktop.closeResults();
    }

    if (mobile && mobile.hasOpenResults() && !mobile.contains(target)) {
      mobile.closeResults();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    desktop?.closeResults();
    mobile?.closeResults();
    if (mobileOverlay && !mobileOverlay.classList.contains('hidden')) {
      closeMobileOverlay();
    }
  });
}

document.addEventListener('DOMContentLoaded', initGlobalSearch);
