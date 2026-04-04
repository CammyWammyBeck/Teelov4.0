import { byId, queryAll, toggleHidden } from '../lib/dom.js';
import { toggleValue } from '../lib/filters.js';
import { escapeHtml, formatNumber, formatShortDate } from '../lib/format.js';
import { getJson } from '../lib/http.js';
import {
  createTournamentsState,
  hydrateFromUrl,
  toApiQuery,
  toUrlQuery,
  TOURNAMENT_MULTI_VALUE_FILTERS,
} from '../state/tournaments_state.js';

const SURFACE_BADGES = {
  Hard: 'bg-blue-100 text-blue-700',
  Clay: 'bg-orange-100 text-orange-700',
  Grass: 'bg-status-success-subtle text-status-success',
};

const STATUS_BADGES = {
  current: 'bg-status-success-subtle text-status-success',
  upcoming: 'bg-surface-muted text-content-secondary',
  completed: 'bg-surface-alt text-content-secondary',
};
const CURRENT_WINDOW_BEFORE_DAYS = 3;
const CURRENT_WINDOW_AFTER_DAYS = 3;

function parseDateOnly(value) {
  if (!value) return null;
  const dateValue = new Date(`${value}T00:00:00`);
  return Number.isNaN(dateValue.getTime()) ? null : dateValue;
}

function startOfToday() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate());
}

function addDays(dateValue, days) {
  const result = new Date(dateValue);
  result.setDate(result.getDate() + days);
  return result;
}

function hasCompletedFinal(item) {
  return Boolean(item?.winner_name || item?.has_completed_final);
}

function hasCompletedMatches(item) {
  return Boolean(item?.has_completed_matches || Number(item?.completed_results) > 0);
}

function normalizeTournamentStatus(item) {
  const today = startOfToday();
  const startDate = parseDateOnly(item?.start_date);
  const endDate = parseDateOnly(item?.end_date);

  if (hasCompletedFinal(item)) {
    return 'completed';
  }

  if (startDate && startDate > addDays(today, CURRENT_WINDOW_BEFORE_DAYS)) {
    return 'upcoming';
  }

  if (startDate && endDate) {
    const currentWindowStart = addDays(startDate, -CURRENT_WINDOW_BEFORE_DAYS);
    const currentWindowEnd = addDays(endDate, CURRENT_WINDOW_AFTER_DAYS);
    if (today >= currentWindowStart && today <= currentWindowEnd && hasCompletedMatches(item)) {
      return 'current';
    }
    if (today > currentWindowEnd) {
      return 'completed';
    }
  }

  if (endDate && today > addDays(endDate, CURRENT_WINDOW_AFTER_DAYS)) {
    return 'completed';
  }

  return item?.status || 'completed';
}

function normalizeTournamentItem(item) {
  return {
    ...item,
    status: normalizeTournamentStatus(item),
  };
}

function withStatus(items, status) {
  return (items || []).map((item) => ({ ...item, status }));
}

function compareDatesDesc(a, b) {
  const aDate = parseDateOnly(a);
  const bDate = parseDateOnly(b);
  const aTime = aDate ? aDate.getTime() : -Infinity;
  const bTime = bDate ? bDate.getTime() : -Infinity;
  return bTime - aTime;
}

function compareDatesAsc(a, b) {
  const aDate = parseDateOnly(a);
  const bDate = parseDateOnly(b);
  const aTime = aDate ? aDate.getTime() : Infinity;
  const bTime = bDate ? bDate.getTime() : Infinity;
  return aTime - bTime;
}

function compareTournamentOrder(a, b) {
  const statusRank = { current: 0, upcoming: 1, completed: 2 };
  const rankDiff = (statusRank[a.status] ?? 9) - (statusRank[b.status] ?? 9);
  if (rankDiff !== 0) return rankDiff;

  if (a.status === 'current') {
    const endDiff = compareDatesAsc(a.end_date, b.end_date);
    if (endDiff !== 0) return endDiff;
    const startDiff = compareDatesAsc(a.start_date, b.start_date);
    if (startDiff !== 0) return startDiff;
  }

  if (a.status === 'upcoming') {
    const startDiff = compareDatesAsc(a.start_date, b.start_date);
    if (startDiff !== 0) return startDiff;
  }

  if (a.status === 'completed') {
    const endDiff = compareDatesDesc(a.end_date, b.end_date);
    if (endDiff !== 0) return endDiff;
    const startDiff = compareDatesDesc(a.start_date, b.start_date);
    if (startDiff !== 0) return startDiff;
  }

  const yearDiff = Number(b.year || 0) - Number(a.year || 0);
  if (yearDiff !== 0) return yearDiff;

  return String(a.tournament_name || '').localeCompare(String(b.tournament_name || ''));
}

function surfaceBadge(surface) {
  if (!surface) return '—';
  const cls = SURFACE_BADGES[surface] || 'bg-surface-muted text-content-secondary';
  return `<span class="inline-flex items-center rounded-full px-2 py-1 text-[11px] font-semibold ${cls}">${escapeHtml(surface)}</span>`;
}

function statusBadge(status) {
  const normalized = String(status || '').toLowerCase();
  const cls = STATUS_BADGES[normalized] || 'bg-surface-muted text-content-secondary';
  const label = normalized ? `${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}` : 'Unknown';
  return `<span class="inline-flex items-center rounded-full px-2 py-1 text-[11px] font-semibold ${cls}">${escapeHtml(label)}</span>`;
}

function formatDateRange(startDate, endDate) {
  const parts = [formatShortDate(startDate), formatShortDate(endDate)].filter(Boolean);
  if (!parts.length) return 'Dates TBC';
  if (parts.length === 1 || parts[0] === parts[1]) return parts[0];
  return `${parts[0]} - ${parts[1]}`;
}

function renderFeaturedList(prefix, items) {
  const loading = byId(`featured-${prefix}-loading`);
  const list = byId(`featured-${prefix}-list`);
  const empty = byId(`featured-${prefix}-empty`);
  if (!loading || !list || !empty) return;

  loading.classList.add('hidden');
  if (!items.length) {
    list.classList.add('hidden');
    empty.classList.remove('hidden');
    return;
  }

  list.innerHTML = items.map((item) => {
    const favouriteHtml = item.favourite_name
      ? `<span class="inline-flex items-center gap-1 text-[11px] font-semibold text-teelo-ink bg-teelo-lime rounded-full px-2 py-0.5 leading-none mt-1">
           <svg class="w-2.5 h-2.5" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
           ${escapeHtml(item.favourite_name)} ${item.favourite_win_pct}%
         </span>`
      : '';
    return `
    <li class="px-5 py-3 hover:bg-surface-hover transition-colors">
      <a href="${escapeHtml(item.url || '#')}" class="block">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <div class="flex items-center gap-2 min-w-0">
              <span class="inline-flex shrink-0 items-center justify-center text-[9px] font-bold text-content-inverse px-1.5 py-0.5 rounded ${escapeHtml(item.badge_color || 'bg-surface-muted')}">${escapeHtml(item.badge_label || 'EVT')}</span>
              <span class="truncate text-sm font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2">${escapeHtml(item.tournament_name || 'Unknown')}</span>
            </div>
            <p class="text-xs text-content-faint mt-1">${escapeHtml(item.city || '')}${item.city && item.year ? ' · ' : ''}${escapeHtml(item.year || '')}</p>
            <p class="text-xs text-content-muted mt-1">${escapeHtml(formatDateRange(item.start_date, item.end_date))}</p>
            ${item.winner_name ? `<p class="text-xs text-content-muted mt-1">Winner: <span class="font-semibold text-teelo-dark">${escapeHtml(item.winner_name)}</span></p>` : ''}
            ${favouriteHtml}
          </div>
          <div class="shrink-0">${surfaceBadge(item.surface)}</div>
        </div>
      </a>
    </li>`;
  }).join('');

  list.classList.remove('hidden');
  empty.classList.add('hidden');
}

function favouriteBar(item) {
  if (!item.favourite_name) return '';
  const pct = Number(item.favourite_win_pct) || 0;
  return `
    <div class="flex items-center gap-2 mt-1.5">
      <span class="inline-flex items-center gap-1 text-[11px] font-semibold text-teelo-ink bg-teelo-lime rounded-full px-2 py-0.5 leading-none shrink-0">
        <svg class="w-2.5 h-2.5" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
        ${escapeHtml(item.favourite_name)}
      </span>
      <div class="flex-1 h-1.5 rounded-full bg-surface-muted overflow-hidden">
        <div class="h-full rounded-full bg-teelo-lime" style="width:${pct}%"></div>
      </div>
      <span class="text-xs font-bold text-teelo-dark shrink-0">${pct}%</span>
    </div>`;
}

function renderTournamentRows(items) {
  return items.map((item) => `
    <tr class="hover:bg-surface-hover transition-colors border-l-4 border-transparent hover:border-teelo-lime">
      <td class="px-5 py-4">
        <div class="min-w-0">
          <div class="flex items-center gap-2 min-w-0">
            <span class="inline-flex shrink-0 items-center justify-center text-[9px] font-bold text-content-inverse px-1.5 py-0.5 rounded ${escapeHtml(item.badge_color || 'bg-surface-muted')}">${escapeHtml(item.badge_label || 'EVT')}</span>
            <a href="${escapeHtml(item.url || '#')}" class="text-sm font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 truncate block">${escapeHtml(item.tournament_name || 'Unknown')}</a>
          </div>
          <p class="text-xs text-content-faint mt-1">${escapeHtml(item.location || 'Location TBC')}</p>
          ${item.winner_name ? `<p class="text-xs text-content-muted mt-1">Winner: <span class="font-semibold text-teelo-dark">${escapeHtml(item.winner_name)}</span></p>` : ''}
          ${favouriteBar(item)}
        </div>
      </td>
      <td class="px-5 py-4">
        <div class="text-sm font-semibold text-teelo-dark">${escapeHtml(item.level || item.tour || '—')}</div>
        <div class="text-xs text-content-faint mt-1">${escapeHtml(item.tour || '')}${item.year ? ` · ${escapeHtml(item.year)}` : ''}</div>
      </td>
      <td class="px-5 py-4">${surfaceBadge(item.surface)}</td>
      <td class="px-5 py-4 text-sm text-content-secondary">${escapeHtml(formatDateRange(item.start_date, item.end_date))}</td>
      <td class="px-5 py-4">${statusBadge(item.status)}</td>
    </tr>
  `).join('');
}

function renderTournamentCards(items) {
  return items.map((item) => `
    <a href="${escapeHtml(item.url || '#')}" class="block px-4 py-4 border-b border-line-subtle last:border-b-0">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 flex-1">
          <div class="flex items-center gap-2 min-w-0">
            <span class="inline-flex shrink-0 items-center justify-center text-[9px] font-bold text-content-inverse px-1.5 py-0.5 rounded ${escapeHtml(item.badge_color || 'bg-surface-muted')}">${escapeHtml(item.badge_label || 'EVT')}</span>
            <span class="text-sm font-semibold text-teelo-dark truncate">${escapeHtml(item.tournament_name || 'Unknown')}</span>
          </div>
          <div class="text-[11px] text-content-faint mt-1">${escapeHtml(item.level || item.tour || '—')}${item.year ? ` · ${escapeHtml(item.year)}` : ''}</div>
          <div class="text-[11px] text-content-muted mt-1">${escapeHtml(item.location || 'Location TBC')}</div>
          <div class="text-[11px] text-content-muted mt-1">${escapeHtml(formatDateRange(item.start_date, item.end_date))}</div>
          ${item.winner_name ? `<div class="text-[11px] text-content-muted mt-1">Winner: <span class="font-semibold text-teelo-dark">${escapeHtml(item.winner_name)}</span></div>` : ''}
          ${favouriteBar(item)}
        </div>
        <div class="flex flex-col items-end gap-2 shrink-0">
          ${statusBadge(item.status)}
          ${surfaceBadge(item.surface)}
        </div>
      </div>
    </a>
  `).join('');
}

export function initTournamentsPage() {
  const tableBody = byId('tournaments-table-body');
  if (!tableBody) return;

  const state = createTournamentsState();
  hydrateFromUrl(state, window.location.search);

  const els = {
    tableBody,
    cardsContainer: byId('tournaments-cards-container'),
    emptyState: byId('empty-state'),
    scrollSentinel: byId('scroll-sentinel'),
    resultsCount: byId('results-count'),
    filterSummary: byId('filter-summary'),
    filterTags: byId('filter-tags'),
    clearAllBtn: byId('clear-all-btn'),
    tournamentSearch: byId('tournament-search'),
    resultsWrapper: byId('tournaments-results-wrapper'),
  };

  if (els.tournamentSearch && state.tournament) {
    els.tournamentSearch.value = state.tournament;
  }

  function syncFilterChipState() {
    queryAll('.filter-chip').forEach((chip) => {
      const { filter, value } = chip.dataset;
      if (filter === 'gender') {
        chip.classList.toggle('active', state.gender === value);
        return;
      }
      if (!filter || !TOURNAMENT_MULTI_VALUE_FILTERS.includes(filter)) return;
      chip.classList.toggle('active', (state[filter] || []).includes(value));
    });
  }

  function syncUrl() {
    const qs = toUrlQuery(state);
    history.replaceState(null, '', `${window.location.pathname}${qs ? `?${qs}` : ''}`);
  }

  function summaryTags() {
    const tags = [];
    if (state.gender) tags.push({ label: `Gender: ${state.gender}`, removeKey: 'gender:' });
    TOURNAMENT_MULTI_VALUE_FILTERS.forEach((key) => {
      state[key].forEach((value) => tags.push({ label: value, removeKey: `${key}:${value}` }));
    });
    if (state.tournament) tags.push({ label: `Tournament: ${state.tournament}`, removeKey: 'tournament:' });
    return tags;
  }

  function renderSummary() {
    const tags = summaryTags();
    if (!els.filterSummary || !els.filterTags) return;
    if (!tags.length) {
      els.filterSummary.classList.add('hidden');
      els.filterTags.innerHTML = '';
      return;
    }
    els.filterSummary.classList.remove('hidden');
    els.filterTags.innerHTML = tags.map((tag) => (
      `<span class="filter-summary-tag">${escapeHtml(tag.label)} <button data-remove="${escapeHtml(tag.removeKey)}">&times;</button></span>`
    )).join('');
    els.filterTags.querySelectorAll('button[data-remove]').forEach((button) => {
      button.addEventListener('click', () => removeFilter(button.dataset.remove || ''));
    });
  }

  function renderBrowse(data, append = false) {
    const tournaments = (data?.tournaments || []).map(normalizeTournamentItem);
    if (!append && tournaments.length === 0) {
      els.tableBody.innerHTML = '';
      els.cardsContainer.innerHTML = '';
      els.emptyState.classList.remove('hidden');
    } else {
      els.emptyState.classList.add('hidden');
      const rowsHtml = renderTournamentRows(tournaments);
      const cardsHtml = renderTournamentCards(tournaments);
      if (append) {
        els.tableBody.insertAdjacentHTML('beforeend', rowsHtml);
        els.cardsContainer.insertAdjacentHTML('beforeend', cardsHtml);
      } else {
        els.tableBody.innerHTML = rowsHtml;
        els.cardsContainer.innerHTML = cardsHtml;
      }
    }

    state.has_more = Boolean(data?.has_more);
    if (els.resultsCount) {
      const total = Number(data?.total) || 0;
      els.resultsCount.textContent = `${formatNumber(total)} edition${total === 1 ? '' : 's'}`;
    }
    toggleHidden(els.scrollSentinel, !state.has_more && (Number(data?.total) || 0) > 0);
    window.lucide?.createIcons?.();
  }

  let abortController = null;

  async function fetchBrowse(append = false) {
    if (abortController) abortController.abort();
    abortController = new AbortController();

    if (!append) {
      state.page = 1;
      if (els.resultsWrapper && els.resultsWrapper.offsetHeight > 0) {
        els.resultsWrapper.style.minHeight = `${Math.ceil(els.resultsWrapper.offsetHeight)}px`;
        els.resultsWrapper.classList.add('opacity-60', 'pointer-events-none', 'transition-opacity', 'duration-150');
      }
    }

    state.loading = true;
    try {
      const data = await getJson(`/api/tournaments?${toApiQuery(state)}`, { signal: abortController.signal });
      renderBrowse(data, append);
    } catch (error) {
      if (error.name === 'AbortError') return;
      console.error(error);
      renderBrowse({ tournaments: [], total: 0, has_more: false }, append);
    } finally {
      state.loading = false;
      if (els.resultsWrapper) {
        els.resultsWrapper.style.minHeight = '';
        els.resultsWrapper.classList.remove('opacity-60', 'pointer-events-none');
      }
    }
  }

  async function fetchFeatured() {
    try {
      const data = await getJson('/api/home/tournaments');
      const normalizedItems = [
        ...(withStatus(data?.in_progress, 'current').map(normalizeTournamentItem)),
        ...(withStatus(data?.upcoming, 'upcoming').map(normalizeTournamentItem)),
        ...(withStatus(data?.recently_completed, 'completed').map(normalizeTournamentItem)),
      ];
      renderFeaturedList('current', normalizedItems.filter((item) => item.status === 'current').sort(compareTournamentOrder));
      renderFeaturedList('upcoming', normalizedItems.filter((item) => item.status === 'upcoming').sort(compareTournamentOrder));
      renderFeaturedList('completed', normalizedItems.filter((item) => item.status === 'completed').sort(compareTournamentOrder));
    } catch (error) {
      console.error(error);
      renderFeaturedList('current', []);
      renderFeaturedList('upcoming', []);
      renderFeaturedList('completed', []);
    }
  }

  function onFilterChange() {
    syncUrl();
    renderSummary();
    fetchBrowse(false);
  }

  function removeFilter(removeKey) {
    const [name, ...rest] = removeKey.split(':');
    const value = rest.join(':');
    if (name === 'gender') {
      state.gender = '';
    } else if (name === 'tournament') {
      state.tournament = '';
      if (els.tournamentSearch) els.tournamentSearch.value = '';
    } else if (TOURNAMENT_MULTI_VALUE_FILTERS.includes(name)) {
      state[name] = (state[name] || []).filter((entry) => entry !== value);
    }
    syncFilterChipState();
    onFilterChange();
  }

  queryAll('.filter-chip').forEach((chip) => {
    chip.addEventListener('click', () => {
      const { filter, value } = chip.dataset;
      if (!filter || !value) return;
      if (filter === 'gender') {
        state.gender = state.gender === value ? '' : value;
        syncFilterChipState();
        onFilterChange();
        return;
      }
      if (!TOURNAMENT_MULTI_VALUE_FILTERS.includes(filter)) return;
      state[filter] = toggleValue(state[filter], value);
      syncFilterChipState();
      onFilterChange();
    });
  });

  byId('more-filters-btn')?.addEventListener('click', () => {
    byId('filter-drawer-overlay')?.classList.add('open');
    window.lucide?.createIcons?.();
  });
  byId('close-drawer-btn')?.addEventListener('click', () => {
    byId('filter-drawer-overlay')?.classList.remove('open');
  });
  byId('apply-drawer-btn')?.addEventListener('click', () => {
    byId('filter-drawer-overlay')?.classList.remove('open');
    onFilterChange();
  });

  els.clearAllBtn?.addEventListener('click', () => {
    Object.assign(state, createTournamentsState());
    if (els.tournamentSearch) els.tournamentSearch.value = '';
    syncFilterChipState();
    onFilterChange();
  });

  els.tournamentSearch?.addEventListener('change', () => {
    state.tournament = els.tournamentSearch.value.trim();
    onFilterChange();
  });

  const io = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting && state.has_more && !state.loading) {
      state.page += 1;
      fetchBrowse(true);
    }
  }, { rootMargin: '240px' });
  if (els.scrollSentinel) io.observe(els.scrollSentinel);

  syncFilterChipState();
  renderSummary();
  fetchFeatured();
  fetchBrowse(false);
}
