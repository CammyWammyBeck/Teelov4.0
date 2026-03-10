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
    matchesWrapper: byId('matches-results-wrapper'),
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

  let abortController = null;

  async function fetchMatches(append = false) {
    // Cancel any in-flight request
    if (abortController) abortController.abort();
    abortController = new AbortController();

    if (!append) {
      state.page = 1;
      // Grey out existing content during filter-triggered reloads
      if (els.matchesWrapper && els.matchesWrapper.offsetHeight > 0) {
        els.matchesWrapper.style.minHeight = `${Math.ceil(els.matchesWrapper.offsetHeight)}px`;
        els.matchesWrapper.classList.add('opacity-60', 'pointer-events-none', 'transition-opacity', 'duration-150');
      }
    }
    state.loading = true;
    try {
      const data = await getJson(`/api/matches?${toApiQuery(state)}`, { signal: abortController.signal });
      state.has_more = !!data.has_more;
      renderMatchesView(els, data, append);
    } catch (e) {
      if (e.name === 'AbortError') return; // superseded by newer request
      console.error(e);
      els.emptyState.classList.remove('hidden');
    } finally {
      state.loading = false;
      if (els.matchesWrapper) {
        els.matchesWrapper.style.minHeight = '';
        els.matchesWrapper.classList.remove('opacity-60', 'pointer-events-none');
      }
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
      if (filter === 'gender') {
        state.gender = state.gender === value ? '' : value;
        // Clear sibling gender chips so only one can appear active
        queryAll('.filter-chip[data-filter="gender"]').forEach((c) => c.classList.remove('active'));
        if (state.gender) chip.classList.add('active');
      } else if (filter === 'date_preset') {
        // Single-select toggle — clear sibling date_preset chips
        const wasActive = state.date_preset === value;
        state.date_preset = wasActive ? '' : value;
        state.date_from = '';
        state.date_to = '';
        queryAll('.filter-chip[data-filter="date_preset"]').forEach((c) => c.classList.remove('active'));
        if (!wasActive) chip.classList.add('active');
        // Show/hide custom date row
        const customRow = byId('custom-date-row');
        if (customRow) customRow.classList.toggle('hidden', state.date_preset !== 'custom');
      } else if (MULTI_VALUE_FILTERS.includes(filter)) {
        state[filter] = toggleValue(state[filter], value);
        chip.classList.toggle('active');
      }
      onFilterChange();
    });
  });

  // Wire up custom date apply button (Bug 3)
  byId('apply-custom-date')?.addEventListener('click', () => {
    const from = byId('date-from')?.value || '';
    const to = byId('date-to')?.value || '';
    state.date_from = from;
    state.date_to = to;
    state.date_preset = '';
    queryAll('.filter-chip[data-filter="date_preset"]').forEach((c) => c.classList.remove('active'));
    onFilterChange();
  });

  // Bug 2: Wire "More Filters" drawer open/close/apply
  const drawerOverlay = byId('filter-drawer-overlay');
  byId('more-filters-btn')?.addEventListener('click', () => {
    drawerOverlay?.classList.add('open');
    window.lucide?.createIcons?.();
  });
  byId('close-drawer-btn')?.addEventListener('click', () => {
    drawerOverlay?.classList.remove('open');
  });
  byId('apply-drawer-btn')?.addEventListener('click', () => {
    drawerOverlay?.classList.remove('open');
    onFilterChange();
  });

  // Bug 6: Wire H2H toggle to show/hide inputs
  const h2hToggle = byId('h2h-toggle');
  const h2hInputs = byId('h2h-inputs');
  h2hToggle?.addEventListener('change', () => {
    h2hInputs?.classList.toggle('hidden', !h2hToggle.checked);
    if (!h2hToggle.checked) {
      state.player_a_id = null;
      state.player_b_id = null;
    }
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
