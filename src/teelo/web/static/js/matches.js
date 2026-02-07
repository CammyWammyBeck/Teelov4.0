/**
 * Matches page filter controller.
 *
 * Manages filter state, API calls, match rendering, infinite scroll,
 * player autocomplete, URL sync, and the "More Filters" drawer.
 *
 * No external dependencies - vanilla JS only.
 * Relies on:
 *   - lucide (global) for icon rendering
 *   - /api/matches for match data
 *   - /api/players/search for player autocomplete
 */
(function () {
    'use strict';

    // =========================================================================
    // State
    // =========================================================================

    const state = {
        gender: '',
        tour: [],
        surface: [],
        level: [],
        round: [],
        status: [],
        player_id: null,
        player_name: '',
        player_a_id: null,
        player_a_name: '',
        player_b_id: null,
        player_b_name: '',
        tournament: '',
        date_from: '',
        date_to: '',
        date_preset: '',
        page: 1,
        per_page: 50,
        total: 0,
        has_more: true,
        loading: false,
    };

    const MULTI_VALUE_FILTERS = ['tour', 'surface', 'level', 'round', 'status'];

    // =========================================================================
    // DOM references (cached after DOMContentLoaded)
    // =========================================================================

    let els = {};

    function cacheDom() {
        els = {
            tableBody: document.getElementById('matches-table-body'),
            cardsContainer: document.getElementById('matches-cards-container'),
            emptyState: document.getElementById('empty-state'),
            scrollSentinel: document.getElementById('scroll-sentinel'),
            resultsCount: document.getElementById('results-count'),
            filterSummary: document.getElementById('filter-summary'),
            filterTags: document.getElementById('filter-tags'),
            clearAllBtn: document.getElementById('clear-all-btn'),
            playerSearch: document.getElementById('player-search'),
            playerDropdown: document.getElementById('player-dropdown'),
            moreFiltersBtn: document.getElementById('more-filters-btn'),
            drawerOverlay: document.getElementById('filter-drawer-overlay'),
            closeDrawerBtn: document.getElementById('close-drawer-btn'),
            applyDrawerBtn: document.getElementById('apply-drawer-btn'),
            tournamentSearch: document.getElementById('tournament-search'),
            h2hToggle: document.getElementById('h2h-toggle'),
            h2hInputs: document.getElementById('h2h-inputs'),
            h2hPlayerA: document.getElementById('h2h-player-a'),
            h2hPlayerB: document.getElementById('h2h-player-b'),
            h2hPlayerADropdown: document.getElementById('h2h-player-a-dropdown'),
            h2hPlayerBDropdown: document.getElementById('h2h-player-b-dropdown'),
            customDateRow: document.getElementById('custom-date-row'),
            dateFrom: document.getElementById('date-from'),
            dateTo: document.getElementById('date-to'),
            applyCustomDate: document.getElementById('apply-custom-date'),
        };
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    function getDisplayFilterValue(key, value) {
        if (key === 'tour') {
            if (value === 'CHALLENGER') return 'ATP Challenger';
            if (value === 'WTA_125') return 'WTA 125';
        }
        return value;
    }

    function getAllowedToursForGender(gender) {
        if (gender === 'men') return ['ATP', 'CHALLENGER', 'ITF'];
        if (gender === 'women') return ['WTA', 'WTA_125', 'ITF'];
        return [];
    }

    function getEffectiveTourFilters() {
        if (!state.gender) {
            return state.tour.slice();
        }

        var allowedTours = getAllowedToursForGender(state.gender);
        if (state.tour.length === 0) {
            return allowedTours;
        }
        return state.tour.filter(function (tour) {
            return allowedTours.indexOf(tour) !== -1;
        });
    }

    function isChipSelected(chip) {
        var filter = chip.dataset.filter;
        var value = chip.dataset.value;

        if (MULTI_VALUE_FILTERS.indexOf(filter) !== -1) {
            return state[filter].indexOf(value) !== -1;
        }
        if (filter === 'date_preset') {
            return state.date_preset === value;
        }
        if (filter === 'gender') {
            return state.gender === value;
        }
        return false;
    }

    function applyGenderToSubcategoryChips() {
        document.querySelectorAll('.filter-chip[data-role="subcategory"]').forEach(function (chip) {
            var chipGender = (chip.dataset.gender || 'all').toLowerCase();
            var isVisible = !state.gender || chipGender === 'all' || chipGender === state.gender;
            var filter = chip.dataset.filter;
            var value = chip.dataset.value;

            chip.classList.toggle('hidden', !isVisible);

            if (!isVisible) {
                if (MULTI_VALUE_FILTERS.indexOf(filter) !== -1) {
                    var idx = state[filter].indexOf(value);
                    if (idx !== -1) state[filter].splice(idx, 1);
                }
                deactivateChip(chip);
                return;
            }

            if (isChipSelected(chip)) {
                activateChip(chip);
            } else {
                deactivateChip(chip);
            }
        });
    }

    /** Format number with comma separators: 5432 -> '5,432'. */
    function formatNumber(n) {
        return n.toLocaleString();
    }

    /** Human-readable label for a date preset. */
    function presetLabel(preset) {
        const labels = {
            '7d': 'Last 7 days',
            '30d': 'Last 30 days',
            '90d': 'Last 90 days',
            'ytd': 'This Year',
        };
        return labels[preset] || preset;
    }

    /** Escape HTML to prevent XSS when inserting user-derived data. */
    function esc(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // =========================================================================
    // URL Sync
    // =========================================================================

    /** Read URL query params into state and apply to chips/inputs. */
    function initFromURL() {
        const params = new URLSearchParams(window.location.search);

        // Array params
        MULTI_VALUE_FILTERS.forEach(function (key) {
            const val = params.get(key);
            state[key] = val ? val.split(',').map(function (s) { return s.trim(); }) : [];
        });

        // Scalar params
        state.gender = params.get('gender') || '';
        state.player_id = params.get('player_id') ? parseInt(params.get('player_id'), 10) : null;
        state.player_name = params.get('player_name') || '';
        state.player_a_id = params.get('player_a_id') ? parseInt(params.get('player_a_id'), 10) : null;
        state.player_a_name = params.get('player_a_name') || '';
        state.player_b_id = params.get('player_b_id') ? parseInt(params.get('player_b_id'), 10) : null;
        state.player_b_name = params.get('player_b_name') || '';
        state.tournament = params.get('tournament') || '';
        state.date_from = params.get('date_from') || '';
        state.date_to = params.get('date_to') || '';
        state.date_preset = params.get('date_preset') || '';

        // Apply state to chip UI
        document.querySelectorAll('.filter-chip').forEach(function (chip) {
            var filter = chip.dataset.filter;
            var value = chip.dataset.value;

            if (isChipSelected(chip)) {
                activateChip(chip);
            } else {
                deactivateChip(chip);
            }
        });

        applyGenderToSubcategoryChips();

        // Apply to inputs
        if (els.playerSearch && state.player_name) {
            els.playerSearch.value = state.player_name;
        }
        if (els.tournamentSearch && state.tournament) {
            els.tournamentSearch.value = state.tournament;
        }
        if (els.dateFrom && state.date_from) {
            els.dateFrom.value = state.date_from;
        }
        if (els.dateTo && state.date_to) {
            els.dateTo.value = state.date_to;
        }

        // Show custom date row if custom dates are set
        if ((state.date_from || state.date_to) && !state.date_preset) {
            els.customDateRow.classList.remove('hidden');
        }

        // H2H state
        if (state.player_a_id && state.player_b_id) {
            els.h2hToggle.checked = true;
            els.h2hInputs.classList.remove('hidden');
            if (els.h2hPlayerA) els.h2hPlayerA.value = state.player_a_name;
            if (els.h2hPlayerB) els.h2hPlayerB.value = state.player_b_name;
        }
    }

    /** Push current state to URL query params. */
    function syncToURL() {
        var params = new URLSearchParams();

        MULTI_VALUE_FILTERS.forEach(function (key) {
            if (state[key].length > 0) {
                params.set(key, state[key].join(','));
            }
        });

        if (state.gender) params.set('gender', state.gender);
        if (state.player_id) {
            params.set('player_id', state.player_id);
            if (state.player_name) params.set('player_name', state.player_name);
        }
        if (state.player_a_id) {
            params.set('player_a_id', state.player_a_id);
            if (state.player_a_name) params.set('player_a_name', state.player_a_name);
        }
        if (state.player_b_id) {
            params.set('player_b_id', state.player_b_id);
            if (state.player_b_name) params.set('player_b_name', state.player_b_name);
        }
        if (state.tournament) params.set('tournament', state.tournament);
        if (state.date_from) params.set('date_from', state.date_from);
        if (state.date_to) params.set('date_to', state.date_to);
        if (state.date_preset) params.set('date_preset', state.date_preset);

        var qs = params.toString();
        var url = window.location.pathname + (qs ? '?' + qs : '');
        history.replaceState(null, '', url);
    }

    // =========================================================================
    // API Calls
    // =========================================================================

    /** Build query string from state for API calls. */
    function buildQueryString() {
        var params = new URLSearchParams();

        var effectiveTours = getEffectiveTourFilters();
        if (effectiveTours.length > 0) {
            params.set('tour', effectiveTours.join(','));
        }
        if (state.gender) {
            params.set('gender', state.gender);
        }

        ['surface', 'level', 'round', 'status'].forEach(function (key) {
            if (state[key].length > 0) params.set(key, state[key].join(','));
        });

        if (state.player_id) params.set('player_id', state.player_id);
        if (state.player_a_id) params.set('player_a_id', state.player_a_id);
        if (state.player_b_id) params.set('player_b_id', state.player_b_id);
        if (state.tournament) params.set('tournament', state.tournament);
        if (state.date_preset && state.date_preset !== 'custom') {
            params.set('date_preset', state.date_preset);
        }
        if (state.date_from) params.set('date_from', state.date_from);
        if (state.date_to) params.set('date_to', state.date_to);

        params.set('page', state.page);
        params.set('per_page', state.per_page);

        return params.toString();
    }

    /** Fetch matches from API and render them. */
    async function fetchMatches(append) {
        if (state.loading) return;
        state.loading = true;

        if (!append) {
            state.page = 1;
            els.tableBody.innerHTML = '';
            els.cardsContainer.innerHTML = '';
            els.emptyState.classList.add('hidden');
            els.scrollSentinel.classList.remove('hidden');
        }

        // Show spinner
        els.scrollSentinel.classList.remove('hidden');

        try {
            var url = '/api/matches?' + buildQueryString();
            var resp = await fetch(url);
            if (!resp.ok) throw new Error('API returned ' + resp.status);
            var data = await resp.json();

            state.total = data.total;
            state.has_more = data.has_more;

            renderMatches(data, append);

            // Update results count
            els.resultsCount.textContent = formatNumber(data.total) + ' match' + (data.total !== 1 ? 'es' : '');

            if (!data.has_more) {
                els.scrollSentinel.classList.add('hidden');
            }
        } catch (err) {
            console.error('Failed to fetch matches:', err);
            if (!append) {
                // Show error state instead of empty state
                els.tableBody.innerHTML = '';
                els.cardsContainer.innerHTML = '';
                els.emptyState.querySelector('h3').textContent = 'Failed to load matches';
                els.emptyState.querySelector('p').textContent = 'Please check your connection and try again.';
                els.emptyState.classList.remove('hidden');
                els.scrollSentinel.classList.add('hidden');
            }
        } finally {
            state.loading = false;
        }
    }

    // =========================================================================
    // Rendering
    // =========================================================================

    /** Render match rows into both desktop table and mobile cards. */
    function renderMatches(data, append) {
        var matches = data.matches || [];
        if (matches.length === 0 && !append) {
            // Reset to default empty state text (may have been changed by error handler)
            els.emptyState.querySelector('h3').textContent = 'No matches found';
            els.emptyState.querySelector('p').textContent = 'Try adjusting your filters';
            els.emptyState.classList.remove('hidden');
            els.scrollSentinel.classList.add('hidden');
            return;
        }

        els.emptyState.classList.add('hidden');

        var tableHtml = data.table_rows_html || '';
        var cardHtml = data.cards_html || '';

        if (append) {
            els.tableBody.insertAdjacentHTML('beforeend', tableHtml);
            els.cardsContainer.insertAdjacentHTML('beforeend', cardHtml);
        } else {
            els.tableBody.innerHTML = tableHtml;
            els.cardsContainer.innerHTML = cardHtml;
        }

        // Render lucide icons in newly added HTML
        if (typeof lucide !== 'undefined') lucide.createIcons();
    }

    // =========================================================================
    // Chip Management
    // =========================================================================

    function activateChip(chip) {
        chip.classList.add('active');
        var classes = (chip.dataset.activeClass || '').split(/\s+/);
        classes.forEach(function (c) { if (c) chip.classList.add(c); });
    }

    function deactivateChip(chip) {
        chip.classList.remove('active');
        var classes = (chip.dataset.activeClass || '').split(/\s+/);
        classes.forEach(function (c) { if (c) chip.classList.remove(c); });
    }

    function setupChipListeners() {
        document.querySelectorAll('.filter-chip').forEach(function (chip) {
            chip.addEventListener('click', function () {
                var filter = chip.dataset.filter;
                var value = chip.dataset.value;

                if (filter === 'gender') {
                    document.querySelectorAll('.filter-chip[data-filter="gender"]').forEach(deactivateChip);

                    if (state.gender === value) {
                        state.gender = '';
                    } else {
                        state.gender = value;
                        activateChip(chip);
                    }

                    applyGenderToSubcategoryChips();
                    onFilterChange();
                } else if (filter === 'date_preset') {
                    handleDatePresetClick(chip, value);
                } else {
                    // Multi-select toggle for tour, surface, level, round, status
                    var idx = state[filter].indexOf(value);
                    if (idx !== -1) {
                        state[filter].splice(idx, 1);
                        deactivateChip(chip);
                    } else {
                        state[filter].push(value);
                        activateChip(chip);
                    }
                    applyGenderToSubcategoryChips();
                    onFilterChange();
                }
            });
        });
    }

    /** Handle date preset chip clicks (exclusive selection). */
    function handleDatePresetClick(chip, value) {
        // Deactivate all date preset chips first
        document.querySelectorAll('.filter-chip[data-filter="date_preset"]').forEach(deactivateChip);

        if (state.date_preset === value) {
            // Clicking the same preset again deactivates it
            state.date_preset = '';
            state.date_from = '';
            state.date_to = '';
            els.customDateRow.classList.add('hidden');
        } else {
            state.date_preset = value;
            activateChip(chip);

            if (value === 'custom') {
                // Show custom date inputs
                els.customDateRow.classList.remove('hidden');
                // Don't trigger filter change yet - wait for Apply
                return;
            } else {
                // Hide custom inputs, clear custom dates
                els.customDateRow.classList.add('hidden');
                state.date_from = '';
                state.date_to = '';
                els.dateFrom.value = '';
                els.dateTo.value = '';
            }
        }
        onFilterChange();
    }

    // =========================================================================
    // Filter Change Handler
    // =========================================================================

    function onFilterChange() {
        state.page = 1;
        state.has_more = true;
        syncToURL();
        renderActiveSummary();
        fetchMatches(false);
    }

    // =========================================================================
    // Active Filter Summary
    // =========================================================================

    function renderActiveSummary() {
        var tags = [];

        if (state.gender) {
            tags.push({
                label: 'Gender: ' + (state.gender === 'men' ? 'Men' : 'Women'),
                removeKey: 'gender:',
            });
        }

        // Array filters
        var filterLabels = {
            tour: 'Tour',
            surface: 'Surface',
            level: 'Level',
            round: 'Round',
            status: 'Status',
        };

        Object.keys(filterLabels).forEach(function (key) {
            state[key].forEach(function (value) {
                var displayValue = key === 'status'
                    ? (value.charAt(0).toUpperCase() + value.slice(1))
                    : getDisplayFilterValue(key, value);
                tags.push({
                    label: displayValue,
                    removeKey: key + ':' + value,
                });
            });
        });

        // Date preset
        if (state.date_preset && state.date_preset !== 'custom') {
            tags.push({ label: presetLabel(state.date_preset), removeKey: 'date_preset:' });
        }

        // Custom date range
        if (state.date_from || state.date_to) {
            var label = (state.date_from || '...') + ' to ' + (state.date_to || '...');
            tags.push({ label: label, removeKey: 'date_range:' });
        }

        // Player
        if (state.player_id && state.player_name) {
            tags.push({ label: 'Player: ' + state.player_name, removeKey: 'player:' });
        }

        // H2H
        if (state.player_a_id && state.player_b_id) {
            tags.push({
                label: 'H2H: ' + (state.player_a_name || '?') + ' vs ' + (state.player_b_name || '?'),
                removeKey: 'h2h:',
            });
        }

        // Tournament
        if (state.tournament) {
            tags.push({ label: 'Tournament: ' + state.tournament, removeKey: 'tournament:' });
        }

        // Show/hide summary
        if (tags.length === 0) {
            els.filterSummary.classList.add('hidden');
            return;
        }
        els.filterSummary.classList.remove('hidden');

        var html = tags.map(function (tag) {
            return '<span class="filter-summary-tag">'
                + esc(tag.label)
                + ' <button data-remove="' + esc(tag.removeKey) + '" class="ml-0.5 hover:text-teelo-dark">&times;</button>'
                + '</span>';
        }).join('');

        els.filterTags.innerHTML = html;

        // Attach remove listeners
        els.filterTags.querySelectorAll('button[data-remove]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                removeFilter(btn.dataset.remove);
            });
        });
    }

    /** Remove a single filter by key (e.g., 'tour:ATP', 'player:', 'date_preset:'). */
    function removeFilter(removeKey) {
        var parts = removeKey.split(':');
        var filterName = parts[0];
        var value = parts.slice(1).join(':');

        if (MULTI_VALUE_FILTERS.indexOf(filterName) !== -1) {
            var idx = state[filterName].indexOf(value);
            if (idx !== -1) state[filterName].splice(idx, 1);
            // Deactivate matching chip
            var chip = document.querySelector('.filter-chip[data-filter="' + filterName + '"][data-value="' + value + '"]');
            if (chip) deactivateChip(chip);
        } else if (filterName === 'gender') {
            state.gender = '';
            document.querySelectorAll('.filter-chip[data-filter="gender"]').forEach(deactivateChip);
            applyGenderToSubcategoryChips();
        } else if (filterName === 'date_preset') {
            state.date_preset = '';
            document.querySelectorAll('.filter-chip[data-filter="date_preset"]').forEach(deactivateChip);
        } else if (filterName === 'date_range') {
            state.date_from = '';
            state.date_to = '';
            state.date_preset = '';
            els.dateFrom.value = '';
            els.dateTo.value = '';
            els.customDateRow.classList.add('hidden');
            document.querySelectorAll('.filter-chip[data-filter="date_preset"]').forEach(deactivateChip);
        } else if (filterName === 'player') {
            state.player_id = null;
            state.player_name = '';
            els.playerSearch.value = '';
        } else if (filterName === 'h2h') {
            state.player_a_id = null;
            state.player_a_name = '';
            state.player_b_id = null;
            state.player_b_name = '';
            els.h2hToggle.checked = false;
            els.h2hInputs.classList.add('hidden');
            if (els.h2hPlayerA) els.h2hPlayerA.value = '';
            if (els.h2hPlayerB) els.h2hPlayerB.value = '';
        } else if (filterName === 'tournament') {
            state.tournament = '';
            els.tournamentSearch.value = '';
        }

        onFilterChange();
    }

    /** Reset all filters to defaults. */
    function clearAllFilters() {
        state.gender = '';
        state.tour = [];
        state.surface = [];
        state.level = [];
        state.round = [];
        state.status = [];
        state.player_id = null;
        state.player_name = '';
        state.player_a_id = null;
        state.player_a_name = '';
        state.player_b_id = null;
        state.player_b_name = '';
        state.tournament = '';
        state.date_from = '';
        state.date_to = '';
        state.date_preset = '';

        // Deactivate all chips
        document.querySelectorAll('.filter-chip').forEach(deactivateChip);
        applyGenderToSubcategoryChips();

        // Clear inputs
        els.playerSearch.value = '';
        els.tournamentSearch.value = '';
        els.dateFrom.value = '';
        els.dateTo.value = '';
        els.customDateRow.classList.add('hidden');
        els.h2hToggle.checked = false;
        els.h2hInputs.classList.add('hidden');
        if (els.h2hPlayerA) els.h2hPlayerA.value = '';
        if (els.h2hPlayerB) els.h2hPlayerB.value = '';

        onFilterChange();
    }

    // =========================================================================
    // Infinite Scroll
    // =========================================================================

    var scrollObserver = null;

    function setupInfiniteScroll() {
        if (!('IntersectionObserver' in window)) return;

        scrollObserver = new IntersectionObserver(function (entries) {
            entries.forEach(function (entry) {
                if (entry.isIntersecting && state.has_more && !state.loading) {
                    state.page++;
                    fetchMatches(true);
                }
            });
        }, { rootMargin: '200px' });

        scrollObserver.observe(els.scrollSentinel);
    }

    // =========================================================================
    // Player Autocomplete
    // =========================================================================

    /**
     * Set up autocomplete on an input element.
     * @param {HTMLInputElement} inputEl - The text input
     * @param {HTMLElement} dropdownEl - The dropdown container
     * @param {function(number, string)} onSelect - Called with (playerId, playerName)
     */
    function setupPlayerAutocomplete(inputEl, dropdownEl, onSelect) {
        if (!inputEl || !dropdownEl) return;

        var debounceTimer = null;

        inputEl.addEventListener('input', function () {
            clearTimeout(debounceTimer);
            var q = inputEl.value.trim();

            if (q.length < 2) {
                dropdownEl.classList.add('hidden');
                dropdownEl.innerHTML = '';
                return;
            }

            debounceTimer = setTimeout(async function () {
                try {
                    var resp = await fetch('/api/players/search?q=' + encodeURIComponent(q) + '&limit=8');
                    if (!resp.ok) return;
                    var data = await resp.json();

                    if (data.players.length === 0) {
                        dropdownEl.classList.add('hidden');
                        dropdownEl.innerHTML = '';
                        return;
                    }

                    dropdownEl.innerHTML = data.players.map(function (p) {
                        return '<div class="autocomplete-item" data-id="' + p.id + '" data-name="' + esc(p.name) + '">'
                            + '<span class="player-name">' + esc(p.name) + '</span>'
                            + '<span class="player-nationality">' + esc(p.nationality || '') + '</span>'
                            + '</div>';
                    }).join('');

                    dropdownEl.classList.remove('hidden');

                    // Attach click listeners to items
                    dropdownEl.querySelectorAll('.autocomplete-item').forEach(function (item) {
                        item.addEventListener('click', function () {
                            var id = parseInt(item.dataset.id, 10);
                            var name = item.dataset.name;
                            inputEl.value = name;
                            dropdownEl.classList.add('hidden');
                            onSelect(id, name);
                        });
                    });
                } catch (err) {
                    console.error('Player search failed:', err);
                }
            }, 300);
        });

        // Close on Escape
        inputEl.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') {
                dropdownEl.classList.add('hidden');
            }
        });

        // Close on outside click
        document.addEventListener('click', function (e) {
            if (!inputEl.contains(e.target) && !dropdownEl.contains(e.target)) {
                dropdownEl.classList.add('hidden');
            }
        });
    }

    // =========================================================================
    // Drawer Management
    // =========================================================================

    function openDrawer() {
        els.drawerOverlay.classList.add('open');
    }

    function closeDrawer() {
        els.drawerOverlay.classList.remove('open');
    }

    // =========================================================================
    // Initialization
    // =========================================================================

    document.addEventListener('DOMContentLoaded', function () {
        cacheDom();
        initFromURL();
        setupChipListeners();
        setupInfiniteScroll();

        // Main player search autocomplete
        setupPlayerAutocomplete(els.playerSearch, els.playerDropdown, function (id, name) {
            state.player_id = id;
            state.player_name = name;
            // Clear H2H if main player is selected
            state.player_a_id = null;
            state.player_a_name = '';
            state.player_b_id = null;
            state.player_b_name = '';
            els.h2hToggle.checked = false;
            els.h2hInputs.classList.add('hidden');
            onFilterChange();
        });

        // Handle clearing the player search input
        els.playerSearch.addEventListener('input', function () {
            if (els.playerSearch.value.trim() === '' && state.player_id) {
                state.player_id = null;
                state.player_name = '';
                onFilterChange();
            }
        });

        // H2H autocompletes
        setupPlayerAutocomplete(els.h2hPlayerA, els.h2hPlayerADropdown, function (id, name) {
            state.player_a_id = id;
            state.player_a_name = name;
            // Clear main player filter
            state.player_id = null;
            state.player_name = '';
            els.playerSearch.value = '';
            if (state.player_b_id) onFilterChange();
        });

        setupPlayerAutocomplete(els.h2hPlayerB, els.h2hPlayerBDropdown, function (id, name) {
            state.player_b_id = id;
            state.player_b_name = name;
            state.player_id = null;
            state.player_name = '';
            els.playerSearch.value = '';
            if (state.player_a_id) onFilterChange();
        });

        // H2H toggle
        els.h2hToggle.addEventListener('change', function () {
            if (els.h2hToggle.checked) {
                els.h2hInputs.classList.remove('hidden');
            } else {
                els.h2hInputs.classList.add('hidden');
                state.player_a_id = null;
                state.player_a_name = '';
                state.player_b_id = null;
                state.player_b_name = '';
                if (els.h2hPlayerA) els.h2hPlayerA.value = '';
                if (els.h2hPlayerB) els.h2hPlayerB.value = '';
                onFilterChange();
            }
        });

        // Drawer
        els.moreFiltersBtn.addEventListener('click', openDrawer);
        els.closeDrawerBtn.addEventListener('click', closeDrawer);
        els.drawerOverlay.addEventListener('click', function (e) {
            if (e.target === els.drawerOverlay) closeDrawer();
        });
        els.applyDrawerBtn.addEventListener('click', function () {
            // Read tournament search value
            state.tournament = els.tournamentSearch.value.trim();
            closeDrawer();
            onFilterChange();
        });

        // Custom date apply
        els.applyCustomDate.addEventListener('click', function () {
            state.date_from = els.dateFrom.value || '';
            state.date_to = els.dateTo.value || '';
            // Clear preset since we're using custom dates
            state.date_preset = '';
            document.querySelectorAll('.filter-chip[data-filter="date_preset"]').forEach(function (chip) {
                if (chip.dataset.value === 'custom') {
                    activateChip(chip);
                } else {
                    deactivateChip(chip);
                }
            });
            onFilterChange();
        });

        // Clear all button
        els.clearAllBtn.addEventListener('click', clearAllFilters);

        // Browser back/forward
        window.addEventListener('popstate', function () {
            initFromURL();
            renderActiveSummary();
            fetchMatches(false);
        });

        // Initial render
        renderActiveSummary();
        fetchMatches(false);
    });
})();
