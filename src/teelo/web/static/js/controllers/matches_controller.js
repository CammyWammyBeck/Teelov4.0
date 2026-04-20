import { byId, queryAll } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { MULTI_VALUE_FILTERS, removeByKey, toggleValue } from '../lib/filters.js';
import { enableMatchRowNavigation } from '../lib/match_row_nav.js';
import { createMatchesState, hydrateFromUrl, toApiQuery, toUrlQuery } from '../state/matches_state.js';
import { renderMatchesView, renderSummaryTags } from '../renderers/matches.js';

function toggleBettingColumns(enabled) {
  document.querySelectorAll('.betting-col, .betting-section').forEach(el => {
    el.classList.toggle('hidden', !enabled);
  });
  const bettingTh = document.getElementById('betting-th');
  if (bettingTh) bettingTh.classList.toggle('hidden', !enabled);
}

function updateEv(matchId, player, odds) {
  document.querySelectorAll(`[data-match-id="${matchId}"][data-ev-player="${player}"]`).forEach(evEl => {
    const input = evEl.closest('[data-prediction-a]') || evEl.closest('tr[data-prediction-a]') || evEl.closest('div[data-prediction-a]');
    if (!input) { evEl.textContent = ''; return; }

    const predA = parseFloat(input.dataset.predictionA);
    const prob = player === 'a' ? predA : 1 - predA;

    if (!odds || odds < 1 || isNaN(prob)) {
      evEl.textContent = '';
      return;
    }

    const ev = (odds * prob) - 1;
    const evPct = (ev * 100).toFixed(1);
    const isPositive = ev > 0;
    evEl.textContent = `${isPositive ? '+' : ''}${evPct}%`;
    evEl.className = `betting-ev text-[10px] font-semibold ${isPositive ? 'text-status-success' : 'text-content-faint'}`;
  });
}

function restoreBettingOdds() {
  const saved = JSON.parse(localStorage.getItem('teelo_betting_odds') || '{}');
  Object.entries(saved).forEach(([key, odds]) => {
    const parts = key.split('_');
    const player = parts.pop();
    const matchId = parts.join('_');
    document.querySelectorAll(`.betting-odds-input[data-match-id="${matchId}"][data-player="${player}"]`).forEach(input => {
      input.value = odds;
    });
    updateEv(matchId, player, odds);
  });
}

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

  // Betting toggle — restore from localStorage
  const bettingToggle = byId('betting-toggle');
  if (bettingToggle && localStorage.getItem('teelo_betting_enabled') === 'true') {
    bettingToggle.checked = true;
  }
  bettingToggle?.addEventListener('change', () => {
    localStorage.setItem('teelo_betting_enabled', bettingToggle.checked);
    toggleBettingColumns(bettingToggle.checked);
  });

  hydrateFromUrl(state, window.location.search);
  if (els.playerSearch && state.player_name) els.playerSearch.value = state.player_name;
  if (els.tournamentSearch && state.tournament) els.tournamentSearch.value = state.tournament;

  // Sync chip active states from hydrated URL state
  const UPCOMING_STATUSES = new Set(['upcoming', 'scheduled']);
  const COMPLETED_STATUSES = new Set(['completed', 'retired', 'walkover', 'default']);

  function toggleCustomDateRows(show) {
    byId('custom-date-row')?.classList.toggle('hidden', !show);
    byId('drawer-custom-date-row')?.classList.toggle('hidden', !show);
  }

  // Fix 4: Dim category chips incompatible with the selected gender
  function updateCategoryChipAvailability() {
    queryAll('.filter-chip[data-role="subcategory"]').forEach(chip => {
      const chipGender = chip.dataset.gender;
      const incompatible = (state.gender === 'men' && chipGender === 'women') ||
                           (state.gender === 'women' && chipGender === 'men');
      chip.classList.toggle('opacity-40', incompatible);
      chip.classList.toggle('pointer-events-none', incompatible);
    });
  }

  function syncChipsFromState() {
    const hasUpcoming = state.status.some(s => UPCOMING_STATUSES.has(s));
    const isCompletedActive = state.status.length === 0 ||
      (state.status.length > 0 && state.status.every(s => COMPLETED_STATUSES.has(s)));
    queryAll('.filter-chip').forEach((chip) => {
      const { filter, value } = chip.dataset;
      let isActive = false;
      if (filter === 'gender') {
        isActive = state.gender === value;
      } else if (filter === 'date_preset') {
        isActive = state.date_preset === value;
      } else if (filter === 'status_group') {
        isActive = value === 'upcoming' ? hasUpcoming : isCompletedActive;
      } else if (MULTI_VALUE_FILTERS.includes(filter)) {
        isActive = state[filter].includes(value);
      }
      chip.classList.toggle('active', isActive);
    });
    toggleCustomDateRows(state.date_preset === 'custom');
    updateCategoryChipAvailability();
  }
  syncChipsFromState();


  function syncUrl() {
    const qs = toUrlQuery(state);
    history.replaceState(null, '', `${window.location.pathname}${qs ? `?${qs}` : ''}`);
  }

  function summaryTags() {
    const tags = [];
    if (state.gender) tags.push({ label: `Gender: ${state.gender}`, removeKey: 'gender:' });
    MULTI_VALUE_FILTERS.filter(k => k !== 'status').forEach((k) => state[k].forEach((v) => tags.push({ label: v, removeKey: `${k}:${v}` })));
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
      toggleBettingColumns(bettingToggle?.checked || false);
      restoreBettingOdds();
    } catch (e) {
      if (e.name === 'AbortError') return; // superseded by newer request
      console.error(e);
      const skeleton = document.getElementById('skeleton-loading');
      if (skeleton) skeleton.classList.add('hidden');
      const errorState = document.getElementById('error-state');
      if (errorState) {
        errorState.classList.remove('hidden');
        els.emptyState.classList.add('hidden');
      } else {
        els.emptyState.classList.remove('hidden');
      }
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
    syncChipsFromState();
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
      if (filter === 'status_group') {
        // Fix 5: set explicit statuses so the URL is always bookmarkable
        state.status = value === 'upcoming' ? ['upcoming', 'scheduled'] : ['completed', 'retired', 'walkover', 'default'];
      } else if (filter === 'gender') {
        state.gender = state.gender === value ? '' : value;
        // Fix 4: dim incompatible category chips
        updateCategoryChipAvailability();
      } else if (filter === 'date_preset') {
        // Single-select toggle — clear sibling date_preset chips
        const wasActive = state.date_preset === value;
        state.date_preset = wasActive ? '' : value;
        state.date_from = '';
        state.date_to = '';
        toggleCustomDateRows(!wasActive && value === 'custom');
      } else if (MULTI_VALUE_FILTERS.includes(filter)) {
        state[filter] = toggleValue(state[filter], value);
        chip.classList.toggle('active');
      }
      onFilterChange();
    });
  });

  // Wire up custom date apply buttons (main bar + drawer)
  function applyCustomDate(fromId, toId) {
    state.date_from = byId(fromId)?.value || '';
    state.date_to = byId(toId)?.value || '';
    state.date_preset = '';
    onFilterChange();
  }
  byId('apply-custom-date')?.addEventListener('click', () => applyCustomDate('date-from', 'date-to'));
  byId('drawer-apply-custom-date')?.addEventListener('click', () => applyCustomDate('drawer-date-from', 'drawer-date-to'));

  // Bug 2: Wire "More Filters" drawer open/close/apply with state isolation
  const drawerOverlay = byId('filter-drawer-overlay');
  let drawerSnapshot = null;

  byId('more-filters-btn')?.addEventListener('click', () => {
    // Snapshot full state before drawer opens (Fix 3)
    drawerSnapshot = {
      gender: state.gender,
      tour: [...state.tour],
      surface: [...state.surface],
      level: [...state.level],
      round: [...state.round],
      status: [...state.status],
      date_preset: state.date_preset,
      date_from: state.date_from,
      date_to: state.date_to,
      tournament: state.tournament,
    };
    // Sync drawer date inputs to current state
    const drawerDateFrom = byId('drawer-date-from');
    const drawerDateTo = byId('drawer-date-to');
    if (drawerDateFrom) drawerDateFrom.value = state.date_from;
    if (drawerDateTo) drawerDateTo.value = state.date_to;
    drawerOverlay?.classList.add('open');
    syncChipsFromState(); // Fix 3: ensure drawer chips reflect current state
    window.lucide?.createIcons?.();
  });
  byId('close-drawer-btn')?.addEventListener('click', () => {
    // Restore full snapshot on cancel
    if (drawerSnapshot) {
      state.gender = drawerSnapshot.gender;
      state.tour = drawerSnapshot.tour;
      state.surface = drawerSnapshot.surface;
      state.level = drawerSnapshot.level;
      state.round = drawerSnapshot.round;
      state.status = drawerSnapshot.status;
      state.date_preset = drawerSnapshot.date_preset;
      state.date_from = drawerSnapshot.date_from;
      state.date_to = drawerSnapshot.date_to;
      state.tournament = drawerSnapshot.tournament;
      if (els.tournamentSearch) els.tournamentSearch.value = state.tournament;
      const drawerDateFrom = byId('drawer-date-from');
      const drawerDateTo = byId('drawer-date-to');
      if (drawerDateFrom) drawerDateFrom.value = state.date_from;
      if (drawerDateTo) drawerDateTo.value = state.date_to;
      drawerSnapshot = null;
      onFilterChange(); // re-fetch with restored state
    }
    drawerOverlay?.classList.remove('open');
  });
  byId('apply-drawer-btn')?.addEventListener('click', () => {
    drawerSnapshot = null;  // Discard snapshot, keep current state
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
    ['date-from', 'date-to', 'drawer-date-from', 'drawer-date-to'].forEach(id => {
      const el = byId(id); if (el) el.value = '';
    });
    toggleCustomDateRows(false);
    syncChipsFromState(); // re-syncs all chips including status_group + category availability
    onFilterChange();
  });

  const DEBOUNCE_MS = 300;

  function debounce(fn, ms) {
    let timer;
    return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); };
  }

  // Reusable player autocomplete wiring
  function wirePlayerAutocomplete(inputEl, dropdownEl, stateKey, { onSelect, onClear } = {}) {
    if (!inputEl || !dropdownEl) return;
    let abort = null;
    const search = debounce(async (query) => {
      if (query.length < 2) { dropdownEl.classList.add('hidden'); return; }
      if (abort) abort.abort();
      abort = new AbortController();
      try {
        const data = await getJson(`/api/players/search?q=${encodeURIComponent(query)}&limit=8`, { signal: abort.signal });
        if (!data.players?.length) { dropdownEl.classList.add('hidden'); return; }
        dropdownEl.innerHTML = data.players.map(p =>
          `<div class="autocomplete-item px-3 py-2 text-sm cursor-pointer hover:bg-surface-hover" data-id="${p.id}" data-name="${p.name}">${p.name}${p.nationality ? ` <span class="text-content-faint text-xs">(${p.nationality})</span>` : ''}</div>`
        ).join('');
        dropdownEl.classList.remove('hidden');
        dropdownEl.querySelectorAll('.autocomplete-item').forEach(item => {
          item.addEventListener('click', () => {
            state[stateKey] = Number(item.dataset.id);
            inputEl.value = item.dataset.name;
            dropdownEl.classList.add('hidden');
            if (onSelect) onSelect(item.dataset);
          });
        });
      } catch (e) {
        if (e.name !== 'AbortError') console.error(e);
      }
    }, DEBOUNCE_MS);
    inputEl.addEventListener('input', () => {
      const val = inputEl.value.trim();
      if (!val) {
        state[stateKey] = null;
        dropdownEl.classList.add('hidden');
        if (onClear) onClear();
        return;
      }
      state[stateKey] = null;
      search(val);
    });
    document.addEventListener('click', (e) => {
      if (!inputEl.contains(e.target) && !dropdownEl.contains(e.target)) {
        dropdownEl.classList.add('hidden');
      }
    });
  }

  // Player autocomplete (main filter)
  if (els.playerSearch) {
    const playerDropdown = byId('player-dropdown');
    if (playerDropdown) {
      wirePlayerAutocomplete(els.playerSearch, playerDropdown, 'player_id', {
        onSelect(dataset) {
          state.player_name = dataset.name;
          onFilterChange();
        },
        onClear() {
          state.player_name = '';
          onFilterChange();
        },
      });

      // While typing, update player_name so text search works
      els.playerSearch.addEventListener('input', () => {
        state.player_name = els.playerSearch.value.trim();
      });

      // Submit on Enter without selection (search by name)
      els.playerSearch.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          playerDropdown.classList.add('hidden');
          state.player_name = els.playerSearch.value.trim();
          state.player_id = null;
          onFilterChange();
        }
      });
    }
  }

  // H2H player autocomplete
  wirePlayerAutocomplete(byId('h2h-player-a'), byId('h2h-player-a-dropdown'), 'player_a_id');
  wirePlayerAutocomplete(byId('h2h-player-b'), byId('h2h-player-b-dropdown'), 'player_b_id');

  // Tournament search (debounced)
  if (els.tournamentSearch) {
    const debouncedTournamentSearch = debounce(() => {
      state.tournament = els.tournamentSearch.value.trim();
      onFilterChange();
    }, DEBOUNCE_MS);
    els.tournamentSearch.addEventListener('input', debouncedTournamentSearch);
  }

  const io = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting && state.has_more && !state.loading) {
      state.page += 1;
      fetchMatches(true);
    }
  }, { rootMargin: '240px' });
  if (els.scrollSentinel) io.observe(els.scrollSentinel);

  // Wire retry button in error state
  document.getElementById('retry-btn')?.addEventListener('click', () => {
    document.getElementById('error-state')?.classList.add('hidden');
    fetchMatches(false);
  });

  renderSummaryTags(els, summaryTags(), removeFilter);
  fetchMatches(false);

    const matchesWrapper = document.getElementById('matches-results-wrapper');
    if (matchesWrapper) {
        enableMatchRowNavigation(matchesWrapper);

        // Betting odds input — EV calculation and localStorage persistence
        matchesWrapper.addEventListener('input', (e) => {
            if (!e.target.classList.contains('betting-odds-input')) return;
            const input = e.target;
            const matchId = input.dataset.matchId;
            const player = input.dataset.player;
            const odds = parseFloat(input.value);

            const saved = JSON.parse(localStorage.getItem('teelo_betting_odds') || '{}');
            if (odds && odds >= 1) {
                saved[`${matchId}_${player}`] = odds;
            } else {
                delete saved[`${matchId}_${player}`];
            }
            localStorage.setItem('teelo_betting_odds', JSON.stringify(saved));

            updateEv(matchId, player, odds);
        });
    }
}
