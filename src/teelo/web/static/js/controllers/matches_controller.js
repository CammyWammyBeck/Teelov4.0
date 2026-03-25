import { byId, queryAll } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { MULTI_VALUE_FILTERS, removeByKey, toggleValue } from '../lib/filters.js';
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

  // Bug 1: Sync chip active states from hydrated URL state
  function syncChipsFromState() {
    queryAll('.filter-chip').forEach((chip) => {
      const { filter, value } = chip.dataset;
      let isActive = false;
      if (filter === 'gender') {
        isActive = state.gender === value;
      } else if (filter === 'date_preset') {
        isActive = state.date_preset === value;
      } else if (MULTI_VALUE_FILTERS.includes(filter)) {
        isActive = state[filter].includes(value);
      }
      chip.classList.toggle('active', isActive);
    });
    // Show custom date row if needed
    const customRow = byId('custom-date-row');
    if (customRow && state.date_preset === 'custom') {
      customRow.classList.remove('hidden');
    }
  }
  syncChipsFromState();

  // Bug 4: Show/hide default status hint
  function updateStatusHint() {
    const statusHint = byId('default-status-hint');
    if (statusHint) {
      statusHint.classList.toggle('hidden', state.status.length > 0);
    }
  }
  updateStatusHint();

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
    updateStatusHint();
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

  // Bug 2: Wire "More Filters" drawer open/close/apply with state isolation
  const drawerOverlay = byId('filter-drawer-overlay');
  let drawerSnapshot = null;

  byId('more-filters-btn')?.addEventListener('click', () => {
    // Snapshot state before drawer opens
    drawerSnapshot = {
      level: [...state.level],
      round: [...state.round],
      status: [...state.status],
      tournament: state.tournament,
    };
    drawerOverlay?.classList.add('open');
    window.lucide?.createIcons?.();
  });
  byId('close-drawer-btn')?.addEventListener('click', () => {
    // Restore snapshot on cancel
    if (drawerSnapshot) {
      state.level = drawerSnapshot.level;
      state.round = drawerSnapshot.round;
      state.status = drawerSnapshot.status;
      state.tournament = drawerSnapshot.tournament;
      if (els.tournamentSearch) els.tournamentSearch.value = state.tournament;
      // Re-sync drawer chips to restored state
      queryAll('.filter-drawer .filter-chip').forEach((chip) => {
        const { filter, value } = chip.dataset;
        if (MULTI_VALUE_FILTERS.includes(filter)) {
          chip.classList.toggle('active', state[filter].includes(value));
        }
      });
      drawerSnapshot = null;
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
    queryAll('.filter-chip').forEach((chip) => chip.classList.remove('active'));
    // Bug 3: Clear date inputs and hide custom date row
    const dateFrom = byId('date-from');
    const dateTo = byId('date-to');
    if (dateFrom) dateFrom.value = '';
    if (dateTo) dateTo.value = '';
    const customRow = byId('custom-date-row');
    if (customRow) customRow.classList.add('hidden');
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

  // Bug 4: Wire the "More Filters" link inside the status hint
  byId('show-all-statuses-btn')?.addEventListener('click', () => {
    drawerOverlay?.classList.add('open');
    window.lucide?.createIcons?.();
  });

  // Wire retry button in error state
  document.getElementById('retry-btn')?.addEventListener('click', () => {
    document.getElementById('error-state')?.classList.add('hidden');
    fetchMatches(false);
  });

  renderSummaryTags(els, summaryTags(), removeFilter);
  fetchMatches(false);

    // Match row click-to-detail navigation
    function handleMatchRowClick(e) {
        // Don't navigate if user clicked a link or betting input inside the row
        if (e.target.closest('a') || e.target.closest('.betting-odds-input')) return;
        const row = e.target.closest('[data-match-url]');
        if (row) {
            window.location.href = row.dataset.matchUrl;
        }
    }
    function handleMatchRowKeydown(e) {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            const row = e.target.closest('[data-match-url]');
            if (row) {
                window.location.href = row.dataset.matchUrl;
            }
        }
    }
    const matchesWrapper = document.getElementById('matches-results-wrapper');
    if (matchesWrapper) {
        matchesWrapper.addEventListener('click', handleMatchRowClick);
        matchesWrapper.addEventListener('keydown', handleMatchRowKeydown);

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
