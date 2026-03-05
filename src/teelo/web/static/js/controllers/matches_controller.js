import { byId, queryAll } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { MULTI_VALUE_FILTERS, removeByKey, toggleValue } from '../lib/filters.js';
import { createMatchesState, hydrateFromUrl, toApiQuery, toUrlQuery } from '../state/matches_state.js';
import { renderMatchesView, renderSummaryTags } from '../renderers/matches.js';

export function initMatchesPage() {
  const state = createMatchesState();
  const els = {
    tableBody: byId('matches-table-body'),
    cardsContainer: byId('matches-cards-container'),
    emptyState: byId('empty-state'),
    scrollSentinel: byId('scroll-sentinel'),
    resultsCount: byId('results-count'),
    filterSummary: byId('filter-summary'),
    filterTags: byId('filter-tags'),
    clearAllBtn: byId('clear-all-btn'),
    playerSearch: byId('player-search'),
    tournamentSearch: byId('tournament-search'),
  };
  if (!els.tableBody) return;

  hydrateFromUrl(state, window.location.search);
  if (els.playerSearch && state.player_name) els.playerSearch.value = state.player_name;
  if (els.tournamentSearch && state.tournament) els.tournamentSearch.value = state.tournament;

  function syncUrl() {
    const qs = toUrlQuery(state);
    history.replaceState(null, '', `${window.location.pathname}${qs ? `?${qs}` : ''}`);
  }

  function summaryTags() {
    const tags = [];
    if (state.gender) tags.push({ label: `Gender: ${state.gender}`, removeKey: 'gender:' });
    MULTI_VALUE_FILTERS.forEach((k) => state[k].forEach((v) => tags.push({ label: v, removeKey: `${k}:${v}` })));
    if (state.player_name) tags.push({ label: `Player: ${state.player_name}`, removeKey: 'player:' });
    if (state.tournament) tags.push({ label: `Tournament: ${state.tournament}`, removeKey: 'tournament:' });
    return tags;
  }

  async function fetchMatches(append = false) {
    if (state.loading) return;
    state.loading = true;
    if (!append) state.page = 1;
    try {
      const data = await getJson(`/api/matches?${toApiQuery(state)}`);
      state.has_more = !!data.has_more;
      renderMatchesView(els, data, append);
    } catch (e) {
      console.error(e);
      els.emptyState.classList.remove('hidden');
    } finally {
      state.loading = false;
    }
  }

  function onFilterChange() {
    syncUrl();
    renderSummaryTags(els, summaryTags(), removeFilter);
    fetchMatches(false);
  }

  function removeFilter(removeKey) {
    if (removeKey === 'player:') {
      state.player_id = null; state.player_name = ''; if (els.playerSearch) els.playerSearch.value = '';
    } else if (removeKey === 'tournament:') {
      state.tournament = ''; if (els.tournamentSearch) els.tournamentSearch.value = '';
    } else {
      Object.assign(state, removeByKey(state, removeKey));
    }
    onFilterChange();
  }

  queryAll('.filter-chip').forEach((chip) => {
    chip.addEventListener('click', () => {
      const { filter, value } = chip.dataset;
      if (filter === 'gender') state.gender = state.gender === value ? '' : value;
      else if (MULTI_VALUE_FILTERS.includes(filter)) state[filter] = toggleValue(state[filter], value);
      chip.classList.toggle('active');
      onFilterChange();
    });
  });

  els.clearAllBtn?.addEventListener('click', () => {
    Object.assign(state, createMatchesState());
    if (els.playerSearch) els.playerSearch.value = '';
    if (els.tournamentSearch) els.tournamentSearch.value = '';
    queryAll('.filter-chip').forEach((chip) => chip.classList.remove('active'));
    onFilterChange();
  });

  els.playerSearch?.addEventListener('change', () => {
    state.player_name = els.playerSearch.value.trim();
    state.player_id = null;
    onFilterChange();
  });

  els.tournamentSearch?.addEventListener('change', () => {
    state.tournament = els.tournamentSearch.value.trim();
    onFilterChange();
  });

  const io = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting && state.has_more && !state.loading) {
      state.page += 1;
      fetchMatches(true);
    }
  }, { rootMargin: '240px' });
  if (els.scrollSentinel) io.observe(els.scrollSentinel);

  renderSummaryTags(els, summaryTags(), removeFilter);
  fetchMatches(false);
}
