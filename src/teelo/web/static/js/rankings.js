/**
 * Rankings page controller.
 *
 * Manages dual infinite-scroll columns (men + women) on desktop
 * and a tabbed single-column view on mobile. Each gender has its
 * own independent pagination state and IntersectionObserver.
 *
 * No external dependencies - vanilla JS only.
 * Relies on:
 *   - lucide (global) for icon rendering
 *   - /api/rankings for ranking data
 */
(function () {
    'use strict';

    // =========================================================================
    // State — independent per gender
    // =========================================================================

    function createGenderState() {
        return {
            page: 0,
            loading: false,
            hasMore: true,
            total: 0,
        };
    }

    var genderState = {
        men: createGenderState(),
        women: createGenderState(),
    };

    // Track which mobile tab is active
    var activeMobileTab = 'men';

    // Whether to include inactive players (no match in last 6 months)
    var includeInactive = false;

    // =========================================================================
    // DOM references
    // =========================================================================

    var els = {};

    function cacheDom() {
        els = {
            // Desktop
            menBody: document.getElementById('men-body'),
            womenBody: document.getElementById('women-body'),
            menSentinel: document.getElementById('men-sentinel'),
            womenSentinel: document.getElementById('women-sentinel'),
            menCount: document.getElementById('men-count'),
            womenCount: document.getElementById('women-count'),

            // Mobile
            mobileMenBody: document.getElementById('mobile-men-body'),
            mobileWomenBody: document.getElementById('mobile-women-body'),
            mobileMenSentinel: document.getElementById('mobile-men-sentinel'),
            mobileWomenSentinel: document.getElementById('mobile-women-sentinel'),
            mobileMenPanel: document.getElementById('mobile-men'),
            mobileWomenPanel: document.getElementById('mobile-women'),

            // Shared
            emptyState: document.getElementById('rankings-empty'),
            tabs: document.querySelectorAll('.rankings-tab'),
            inactiveToggle: document.getElementById('include-inactive-toggle'),
        };
    }

    // =========================================================================
    // API
    // =========================================================================

    /**
     * Fetch a page of rankings for a given gender and append to tables.
     * Populates both desktop and mobile tbodies for that gender.
     */
    async function fetchRankings(gender) {
        var st = genderState[gender];
        if (st.loading || !st.hasMore) return;

        st.loading = true;
        st.page++;

        try {
            var url = '/api/rankings?gender=' + gender
                    + '&page=' + st.page
                    + '&per_page=50'
                    + (includeInactive ? '&include_inactive=true' : '');
            var resp = await fetch(url);
            if (!resp.ok) throw new Error('API returned ' + resp.status);
            var data = await resp.json();

            st.total = data.total;
            st.hasMore = data.has_more;

            // Desktop body
            var desktopBody = gender === 'men' ? els.menBody : els.womenBody;
            desktopBody.insertAdjacentHTML('beforeend', data.table_rows_html);

            // Mobile body — render simplified rows (rank, name, elo only)
            var mobileBody = gender === 'men' ? els.mobileMenBody : els.mobileWomenBody;
            var mobileHtml = data.players.map(function (p) {
                return '<tr class="border-b border-gray-50 hover:bg-[#CCFF00]/10 transition-colors duration-75">'
                    + '<td class="px-3 py-2.5 text-sm text-gray-400 font-bold tabular-nums text-right">' + p.rank + '</td>'
                    + '<td class="px-3 py-2.5">'
                    + '<span class="text-sm font-medium text-teelo-dark">' + esc(p.name) + '</span>'
                    + (p.nationality ? '<span class="text-xs text-gray-400 ml-1 font-mono">' + esc(p.nationality) + '</span>' : '')
                    + '</td>'
                    + '<td class="px-3 py-2.5 text-right font-bold text-sm tabular-nums text-teelo-dark">' + p.rating + '</td>'
                    + '</tr>';
            }).join('');
            mobileBody.insertAdjacentHTML('beforeend', mobileHtml);

            // Update count labels
            var countEl = gender === 'men' ? els.menCount : els.womenCount;
            if (countEl) countEl.textContent = data.total.toLocaleString() + ' players';

            // Hide sentinel if no more data
            if (!data.has_more) {
                hideSentinel(gender);
            }

            // Re-render lucide icons in case any were added
            if (typeof lucide !== 'undefined') lucide.createIcons();
        } catch (err) {
            console.error('Failed to fetch ' + gender + ' rankings:', err);
            hideSentinel(gender);
        } finally {
            st.loading = false;
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Escape HTML to prevent XSS. */
    function esc(str) {
        if (!str) return '';
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    /** Hide loading sentinels for a gender (both desktop and mobile). */
    function hideSentinel(gender) {
        if (gender === 'men') {
            els.menSentinel.classList.add('hidden');
            els.mobileMenSentinel.classList.add('hidden');
        } else {
            els.womenSentinel.classList.add('hidden');
            els.mobileWomenSentinel.classList.add('hidden');
        }
    }

    /** Show loading sentinels for a gender (both desktop and mobile). */
    function showSentinel(gender) {
        if (gender === 'men') {
            els.menSentinel.classList.remove('hidden');
            els.mobileMenSentinel.classList.remove('hidden');
        } else {
            els.womenSentinel.classList.remove('hidden');
            els.mobileWomenSentinel.classList.remove('hidden');
        }
    }

    /**
     * Reset pagination state and clear tables for both genders,
     * then reload from page 1. Used when the inactive toggle changes.
     */
    function resetAndReload() {
        ['men', 'women'].forEach(function (gender) {
            genderState[gender] = createGenderState();

            // Clear table bodies
            var desktopBody = gender === 'men' ? els.menBody : els.womenBody;
            var mobileBody = gender === 'men' ? els.mobileMenBody : els.mobileWomenBody;
            desktopBody.innerHTML = '';
            mobileBody.innerHTML = '';

            // Show sentinels again
            showSentinel(gender);
        });

        // Fetch fresh data for both
        fetchRankings('men');
        fetchRankings('women');
    }

    // =========================================================================
    // Infinite Scroll
    // =========================================================================

    var observers = [];

    function setupInfiniteScroll() {
        if (!('IntersectionObserver' in window)) return;

        // Desktop observers — always active
        var menObs = new IntersectionObserver(function (entries) {
            if (entries[0].isIntersecting) fetchRankings('men');
        }, { rootMargin: '300px' });
        menObs.observe(els.menSentinel);
        observers.push(menObs);

        var womenObs = new IntersectionObserver(function (entries) {
            if (entries[0].isIntersecting) fetchRankings('women');
        }, { rootMargin: '300px' });
        womenObs.observe(els.womenSentinel);
        observers.push(womenObs);

        // Mobile observers — only the active tab's sentinel triggers
        var mobileMenObs = new IntersectionObserver(function (entries) {
            if (entries[0].isIntersecting && activeMobileTab === 'men') fetchRankings('men');
        }, { rootMargin: '300px' });
        mobileMenObs.observe(els.mobileMenSentinel);
        observers.push(mobileMenObs);

        var mobileWomenObs = new IntersectionObserver(function (entries) {
            if (entries[0].isIntersecting && activeMobileTab === 'women') fetchRankings('women');
        }, { rootMargin: '300px' });
        mobileWomenObs.observe(els.mobileWomenSentinel);
        observers.push(mobileWomenObs);
    }

    // =========================================================================
    // Mobile Tabs
    // =========================================================================

    var TAB_ACTIVE_CLASSES = ['bg-teelo-lime', 'text-teelo-dark', 'shadow-sm'];
    var TAB_INACTIVE_CLASSES = ['text-gray-400'];

    function activateTab(tab) {
        TAB_INACTIVE_CLASSES.forEach(function (c) { tab.classList.remove(c); });
        TAB_ACTIVE_CLASSES.forEach(function (c) { tab.classList.add(c); });
    }

    function deactivateTab(tab) {
        TAB_ACTIVE_CLASSES.forEach(function (c) { tab.classList.remove(c); });
        TAB_INACTIVE_CLASSES.forEach(function (c) { tab.classList.add(c); });
    }

    function setupTabs() {
        els.tabs.forEach(function (tab) {
            tab.addEventListener('click', function () {
                var gender = tab.dataset.gender;
                if (gender === activeMobileTab) return;

                activeMobileTab = gender;

                // Update tab styles
                els.tabs.forEach(function (t) { deactivateTab(t); });
                activateTab(tab);

                // Show/hide panels
                if (gender === 'men') {
                    els.mobileMenPanel.classList.remove('hidden');
                    els.mobileWomenPanel.classList.add('hidden');
                } else {
                    els.mobileMenPanel.classList.add('hidden');
                    els.mobileWomenPanel.classList.remove('hidden');
                }
            });
        });
    }

    // =========================================================================
    // Initialization
    // =========================================================================

    document.addEventListener('DOMContentLoaded', function () {
        cacheDom();
        setupTabs();
        setupInfiniteScroll();

        // Inactive players toggle
        els.inactiveToggle.addEventListener('change', function () {
            includeInactive = els.inactiveToggle.checked;
            resetAndReload();
        });

        // Initial load: fetch page 1 for both genders in parallel
        fetchRankings('men');
        fetchRankings('women');
    });
})();
