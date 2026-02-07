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

    /** Map tour code to badge background class. */
    function getTourBgClass(tour) {
        const map = {
            ATP: 'bg-[#002865]',
            WTA: 'bg-[#E30066]',
            CHALLENGER: 'bg-[#006B3F]',
            ITF: 'bg-gray-600',
        };
        return map[tour] || 'bg-gray-400';
    }

    /** Short label for tour badges. */
    function getTourLabel(tour) {
        if (tour === 'CHALLENGER') return 'CHL';
        return tour || '?';
    }

    /** Map surface to text color class. */
    function getSurfaceColorClass(surface) {
        const map = {
            Hard: 'text-[#3B82F6]',
            Clay: 'text-[#EA580C]',
            Grass: 'text-green-600',
        };
        return map[surface] || '';
    }

    /** Format 'YYYY-MM-DD' to '26 Jan' style. */
    function formatDate(dateStr) {
        if (!dateStr) return '';
        const d = new Date(dateStr + 'T00:00:00'); // avoid timezone shift
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return d.getDate() + ' ' + months[d.getMonth()];
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
        ['tour', 'surface', 'level', 'round', 'status'].forEach(function (key) {
            const val = params.get(key);
            state[key] = val ? val.split(',').map(function (s) { return s.trim(); }) : [];
        });

        // Scalar params
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

            if (['tour', 'surface', 'level', 'round', 'status'].indexOf(filter) !== -1) {
                if (state[filter].indexOf(value) !== -1) {
                    activateChip(chip);
                } else {
                    deactivateChip(chip);
                }
            } else if (filter === 'date_preset') {
                if (state.date_preset === value) {
                    activateChip(chip);
                } else {
                    deactivateChip(chip);
                }
            }
        });

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

        ['tour', 'surface', 'level', 'round', 'status'].forEach(function (key) {
            if (state[key].length > 0) {
                params.set(key, state[key].join(','));
            }
        });

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

        ['tour', 'surface', 'level', 'round', 'status'].forEach(function (key) {
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

            renderMatches(data.matches, append);

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
    function renderMatches(matches, append) {
        if (matches.length === 0 && !append) {
            // Reset to default empty state text (may have been changed by error handler)
            els.emptyState.querySelector('h3').textContent = 'No matches found';
            els.emptyState.querySelector('p').textContent = 'Try adjusting your filters';
            els.emptyState.classList.remove('hidden');
            els.scrollSentinel.classList.add('hidden');
            return;
        }

        els.emptyState.classList.add('hidden');

        var tableHtml = '';
        var cardHtml = '';

        matches.forEach(function (m) {
            var isWinnerA = m.winner_id && m.winner_id === m.player_a.id;
            var isWinnerB = m.winner_id && m.winner_id === m.player_b.id;
            var tourBg = getTourBgClass(m.tour);
            var tourLabel = getTourLabel(m.tour);
            var surfaceCls = getSurfaceColorClass(m.surface);
            var dateDisplay = formatDate(m.match_date);

            // Desktop table row
            tableHtml += '<tr class="hover:bg-gray-50/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime">'
                + '<td class="px-5 py-3">'
                +   '<div class="flex items-center gap-2">'
                +     '<span class="' + tourBg + ' text-white text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">' + esc(tourLabel) + '</span>'
                +     '<div class="min-w-0">'
                +       '<span class="text-sm font-semibold text-teelo-dark truncate block" title="' + esc(m.tournament_name) + '">' + esc(m.tournament_name || 'Unknown') + '</span>'
                +       '<span class="text-xs text-gray-400">' + esc(m.round || '') + ' · ' + esc(m.tournament_level || '') + ' · <span class="' + surfaceCls + '">' + esc(m.surface || '') + '</span></span>'
                +     '</div>'
                +   '</div>'
                + '</td>'
                + '<td class="px-5 py-3 text-right">'
                +   '<div class="flex items-center justify-end gap-2">'
                +     '<span class="text-sm ' + (isWinnerA ? 'text-teelo-dark font-bold' : 'text-gray-400') + '">' + esc(m.player_a.name) + '</span>'
                +     (isWinnerA ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '')
                +   '</div>'
                + '</td>'
                + '<td class="px-5 py-3 text-center">'
                +   '<span class="inline-block px-2.5 py-1 bg-gray-50 rounded-md text-xs font-mono text-teelo-dark font-semibold whitespace-nowrap group-hover:bg-teelo-lime/10 transition-colors">' + esc(m.score || '') + '</span>'
                + '</td>'
                + '<td class="px-5 py-3">'
                +   '<div class="flex items-center gap-2">'
                +     (isWinnerB ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '')
                +     '<span class="text-sm ' + (isWinnerB ? 'text-teelo-dark font-bold' : 'text-gray-400') + '">' + esc(m.player_b.name) + '</span>'
                +   '</div>'
                + '</td>'
                + '<td class="px-5 py-3 text-right">'
                +   '<span class="text-xs text-gray-400 whitespace-nowrap">' + esc(dateDisplay) + '</span>'
                + '</td>'
                + '</tr>';

            // Mobile card
            cardHtml += '<div class="px-4 py-3 hover:bg-gray-50/50 transition-colors">'
                + '<div class="flex items-center justify-between mb-2">'
                +   '<div class="flex items-center gap-2 min-w-0">'
                +     '<span class="' + tourBg + ' text-white text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">' + esc(tourLabel) + '</span>'
                +     '<span class="text-sm font-semibold text-teelo-dark truncate">' + esc(m.tournament_name || 'Unknown') + '</span>'
                +   '</div>'
                +   '<span class="text-xs text-gray-400 flex-shrink-0 ml-2">' + esc(dateDisplay) + '</span>'
                + '</div>'
                + '<div class="text-xs text-gray-400 mb-2.5 pl-[42px]">' + esc(m.round || '') + ' · ' + esc(m.tournament_level || '') + ' · <span class="' + surfaceCls + '">' + esc(m.surface || '') + '</span></div>'
                + '<div class="flex items-center gap-3 pl-[42px]">'
                +   '<div class="flex-1 min-w-0">'
                +     '<div class="flex items-center gap-1.5 ' + (isWinnerA ? 'text-teelo-dark font-bold' : 'text-gray-400') + ' text-sm">'
                +       (isWinnerA ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '')
                +       '<span class="truncate">' + esc(m.player_a.name) + '</span>'
                +     '</div>'
                +     '<div class="flex items-center gap-1.5 ' + (isWinnerB ? 'text-teelo-dark font-bold' : 'text-gray-400') + ' text-sm mt-0.5">'
                +       (isWinnerB ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '')
                +       '<span class="truncate">' + esc(m.player_b.name) + '</span>'
                +     '</div>'
                +   '</div>'
                +   '<span class="px-2.5 py-1 bg-gray-50 rounded-md text-xs font-mono text-teelo-dark font-semibold whitespace-nowrap flex-shrink-0">' + esc(m.score || '') + '</span>'
                + '</div>'
                + '</div>';
        });

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

                if (filter === 'date_preset') {
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
                var displayValue = key === 'status' ? (value.charAt(0).toUpperCase() + value.slice(1)) : value;
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

        if (['tour', 'surface', 'level', 'round', 'status'].indexOf(filterName) !== -1) {
            var idx = state[filterName].indexOf(value);
            if (idx !== -1) state[filterName].splice(idx, 1);
            // Deactivate matching chip
            var chip = document.querySelector('.filter-chip[data-filter="' + filterName + '"][data-value="' + value + '"]');
            if (chip) deactivateChip(chip);
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
