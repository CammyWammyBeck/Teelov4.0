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

function renderBadge(label, tour) {
  const style = tourStyle(tour);
  return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold" style="background:${style.bg};color:${style.label};">${escapeHtml(label || '')}</span>`;
}

function renderPlayers(players) {
  if (!players.length) {
    return '<div class="rounded-xl border border-dashed border-gray-200 p-6 text-sm text-gray-400">No players found.</div>';
  }
  return players
    .map((player) => `
      <a href="${escapeHtml(player.url || '#')}" class="block rounded-xl border border-gray-100 bg-white p-4 hover:border-gray-200 hover:shadow-sm transition-all">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h3 class="text-sm font-semibold text-teelo-dark truncate">${escapeHtml(player.name || 'Unknown Player')}</h3>
            <p class="text-xs text-gray-500 mt-1">${escapeHtml(player.nationality_ioc || 'Nationality unavailable')}</p>
          </div>
          ${renderBadge(player.tour || 'ATP', player.tour || 'ATP')}
        </div>
      </a>
    `)
    .join('');
}

function renderTournaments(tournaments) {
  if (!tournaments.length) {
    return '<div class="rounded-xl border border-dashed border-gray-200 p-6 text-sm text-gray-400">No tournaments found.</div>';
  }
  return tournaments
    .map((tournament) => {
      const location = [tournament.city, tournament.country].filter((v) => String(v || '').trim()).join(', ');
      const level = tournament.level || 'Level unavailable';
      return `
        <a href="${escapeHtml(tournament.url || '#')}" class="block rounded-xl border border-gray-100 bg-white p-4 hover:border-gray-200 hover:shadow-sm transition-all">
          <div class="flex items-start justify-between gap-3">
            <div class="min-w-0">
              <h3 class="text-sm font-semibold text-teelo-dark truncate">${escapeHtml(tournament.name || 'Unknown Tournament')}</h3>
              <p class="text-xs text-gray-500 mt-1">${escapeHtml(location || 'Location unavailable')}</p>
              <p class="text-xs text-gray-400 mt-0.5">${escapeHtml(level)}</p>
            </div>
            ${renderBadge(tournament.tour || 'ITF', tournament.tour || 'ITF')}
          </div>
        </a>
      `;
    })
    .join('');
}

function setLoading(playersContainer, tournamentsContainer) {
  const loadingHtml = '<div class="rounded-xl border border-gray-100 bg-white p-6 text-sm text-gray-400">Loading…</div>';
  playersContainer.innerHTML = loadingHtml;
  tournamentsContainer.innerHTML = loadingHtml;
}

function initSearchResultsPage() {
  const input = byId('search-results-input');
  const playersContainer = byId('search-results-players');
  const tournamentsContainer = byId('search-results-tournaments');
  const playersCount = byId('search-results-players-count');
  const tournamentsCount = byId('search-results-tournaments-count');

  if (!input || !playersContainer || !tournamentsContainer || !playersCount || !tournamentsCount) return;

  const state = {
    timer: null,
    requestId: 0,
  };

  async function runSearch(query, requestId) {
    if (query.length < 2) {
      playersCount.textContent = '0';
      tournamentsCount.textContent = '0';
      playersContainer.innerHTML = '<div class="rounded-xl border border-dashed border-gray-200 p-6 text-sm text-gray-400">Type at least 2 characters.</div>';
      tournamentsContainer.innerHTML = '<div class="rounded-xl border border-dashed border-gray-200 p-6 text-sm text-gray-400">Type at least 2 characters.</div>';
      return;
    }

    setLoading(playersContainer, tournamentsContainer);

    try {
      const data = await getJson(`/api/search?q=${encodeURIComponent(query)}&full=true`);
      if (requestId !== state.requestId) return;

      const players = Array.isArray(data?.players) ? data.players : [];
      const tournaments = Array.isArray(data?.tournaments) ? data.tournaments : [];
      const playerCountValue = Number(data?.player_count);
      const tournamentCountValue = Number(data?.tournament_count);
      const playerHasMore = Boolean(data?.player_has_more);
      const tournamentHasMore = Boolean(data?.tournament_has_more);

      const displayedPlayerCount = Number.isFinite(playerCountValue) ? playerCountValue : players.length;
      const displayedTournamentCount = Number.isFinite(tournamentCountValue) ? tournamentCountValue : tournaments.length;
      playersCount.textContent = `${displayedPlayerCount}${playerHasMore ? '+' : ''}`;
      tournamentsCount.textContent = `${displayedTournamentCount}${tournamentHasMore ? '+' : ''}`;

      playersContainer.innerHTML = renderPlayers(players);
      tournamentsContainer.innerHTML = renderTournaments(tournaments);
    } catch {
      if (requestId !== state.requestId) return;
      playersCount.textContent = '0';
      tournamentsCount.textContent = '0';
      playersContainer.innerHTML = '<div class="rounded-xl border border-dashed border-red-200 bg-red-50/30 p-6 text-sm text-red-600">Failed to load player results.</div>';
      tournamentsContainer.innerHTML = '<div class="rounded-xl border border-dashed border-red-200 bg-red-50/30 p-6 text-sm text-red-600">Failed to load tournament results.</div>';
    }
  }

  function queueSearch() {
    const query = (input.value || '').trim();
    window.clearTimeout(state.timer);

    const url = new URL(window.location.href);
    if (query) url.searchParams.set('q', query);
    else url.searchParams.delete('q');
    window.history.replaceState({}, '', `${url.pathname}${url.search ? `?${url.searchParams.toString()}` : ''}`);

    const requestId = ++state.requestId;
    state.timer = window.setTimeout(() => runSearch(query, requestId), DEBOUNCE_MS);
  }

  input.addEventListener('input', queueSearch);
  queueSearch();
}

document.addEventListener('DOMContentLoaded', initSearchResultsPage);
