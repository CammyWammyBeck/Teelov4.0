import { byId, queryAll, setSectionLoading, toggleHidden } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { escapeHtml, slugifyName } from '../lib/format.js';
import { hydrateMatchTimes } from '../lib/time.js';
import { enableMatchRowNavigation } from '../lib/match_row_nav.js';
import { renderBracket } from '../renderers/bracket.js';

const TAB_ACTIVE_CLASSES = ['border-b-2', 'border-teelo-lime', 'text-teelo-dark', 'font-semibold'];
const TAB_INACTIVE_CLASSES = ['border-b-2', 'border-transparent', 'text-content-muted', 'hover:text-content-secondary'];

function surfaceClass(surface) {
  if (surface === 'Hard') return 'surface-text-hard';
  if (surface === 'Clay') return 'surface-text-clay';
  if (surface === 'Grass') return 'surface-text-grass';
  if (surface === 'Indoor') return 'surface-text-indoor';
  return '';
}

function renderPlayerLink(name, playerId, className = '', playerUrl = null) {
  const safeName = escapeHtml(name || '—');
  if (Number.isFinite(Number(playerId))) {
    const fallback = `/players/${encodeURIComponent(String(playerId))}/${slugifyName(name)}`;
    return `<a href="${escapeHtml(playerUrl || fallback)}" class="${className} hover:underline decoration-teelo-lime decoration-2">${safeName}</a>`;
  }
  return `<span class="${className}">${safeName}</span>`;
}

export function initTournamentDetailPage() {
  const root = byId('tournament-page-root');
  if (!root) return;

  const tour = root.dataset.tour || '';
  const tournamentCode = root.dataset.tournamentCode || '';
  const year = root.dataset.year || '';
  const hasDraw = root.dataset.hasDraw === 'true';
  const hasForecast = root.dataset.hasForecast === 'true';
  if (!tour || !tournamentCode || !year) return;

  const state = {
    activeTab: 'matches',
    matchStatus: 'completed',
    matchPage: 1,
    drawLoaded: false,
    forecastLoaded: false,
    editionsLoaded: false,
    matchesLoading: false,
  };

  function setTabClasses(button, active) {
    if (!button) return;
    TAB_ACTIVE_CLASSES.forEach((cls) => button.classList.toggle(cls, active));
    TAB_INACTIVE_CLASSES.forEach((cls) => button.classList.toggle(cls, !active));
  }

  function setActiveTab(tab) {
    let normalized = 'matches';
    if (tab === 'draw' && hasDraw) normalized = 'draw';
    else if (tab === 'forecast' && hasForecast) normalized = 'forecast';
    state.activeTab = normalized;

    queryAll('[data-tab]', root).forEach((button) => {
      setTabClasses(button, button.dataset.tab === normalized);
    });

    toggleHidden(byId('tab-panel-matches'), normalized !== 'matches');
    if (hasDraw) {
      toggleHidden(byId('tab-panel-draw'), normalized !== 'draw');
    }
    if (hasForecast) {
      toggleHidden(byId('tab-panel-forecast'), normalized !== 'forecast');
    }

    if (normalized === 'draw' && hasDraw && !state.drawLoaded) {
      loadDraw();
    }
    if (normalized === 'forecast' && hasForecast && !state.forecastLoaded) {
      loadForecast();
    }
  }

  function renderMatches(data) {
    const tableBody = byId('tournament-matches-table-body');
    const cardsContainer = byId('tournament-matches-cards-container');
    const emptyState = byId('tournament-matches-empty');
    const pagination = byId('matches-pagination');
    if (!tableBody || !cardsContainer || !emptyState || !pagination) return;

    const matches = data?.matches || [];
    const total = Number(data?.total) || 0;
    const perPage = Number(data?.per_page) || 10;
    const totalPages = Math.max(1, Math.ceil(total / perPage));
    const hasMore = Boolean(data?.has_more);
    const showPagination = hasMore || state.matchPage > 1;
    if (!matches.length) {
      tableBody.innerHTML = '';
      cardsContainer.innerHTML = '';
      emptyState.classList.remove('hidden');
    } else {
      emptyState.classList.add('hidden');
      tableBody.innerHTML = data?.table_rows_html || '';
      cardsContainer.innerHTML = data?.cards_html || '';
    }

    if (!showPagination) {
      pagination.classList.add('hidden');
      pagination.innerHTML = '';
      hydrateMatchTimes(tableBody);
      hydrateMatchTimes(cardsContainer);
      window.lucide?.createIcons?.();
      return;
    }

    pagination.className = 'flex items-center justify-center gap-4 pt-4';
    pagination.innerHTML = `
      <button
        type="button"
        data-pagination="previous"
        class="px-3 py-1.5 text-sm rounded-lg border border-line hover:bg-surface-hover disabled:opacity-40 disabled:cursor-not-allowed"
        ${state.matchPage <= 1 ? 'disabled' : ''}
      >
        Previous
      </button>
      <span class="text-sm text-content-muted">Page ${state.matchPage} of ${totalPages}</span>
      <button
        type="button"
        data-pagination="next"
        class="px-3 py-1.5 text-sm rounded-lg border border-line hover:bg-surface-hover disabled:opacity-40 disabled:cursor-not-allowed"
        ${!hasMore ? 'disabled' : ''}
      >
        Next
      </button>
    `;

    pagination.querySelector('[data-pagination="previous"]')?.addEventListener('click', () => {
      if (state.matchPage <= 1) return;
      state.matchPage -= 1;
      loadMatches();
    });

    pagination.querySelector('[data-pagination="next"]')?.addEventListener('click', () => {
      if (!hasMore) return;
      state.matchPage += 1;
      loadMatches();
    });

    hydrateMatchTimes(tableBody);
    hydrateMatchTimes(cardsContainer);
    window.lucide?.createIcons?.();
  }

  async function loadMatches() {
    if (state.matchesLoading) return;
    state.matchesLoading = true;
    const matchesContent = byId('matches-content');
    const matchesLoading = byId('matches-loading');
    const currentHeight = matchesContent ? matchesContent.getBoundingClientRect().height : 0;

    if (matchesContent && currentHeight > 0) {
      matchesContent.style.minHeight = `${Math.ceil(currentHeight)}px`;
    }
    if (matchesContent) {
      matchesContent.classList.remove('hidden');
      matchesContent.classList.add('opacity-60', 'pointer-events-none', 'transition-opacity', 'duration-150');
    }
    matchesLoading?.classList.add('hidden');

    const qs = new URLSearchParams({
      status: state.matchStatus,
      page: String(state.matchPage),
      per_page: '10',
    });

    try {
      const data = await getJson(
        `/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(year)}/matches?${qs.toString()}`
      );
      renderMatches(data);
    } catch (error) {
      console.error(error);
      renderMatches({ matches: [] });
    } finally {
      state.matchesLoading = false;
      if (matchesContent) {
        matchesContent.style.minHeight = '';
        matchesContent.classList.remove('opacity-60', 'pointer-events-none');
      }
      matchesLoading?.classList.add('hidden');
    }
  }

  async function loadDraw() {
    setSectionLoading('draw-loading', ['draw-content'], true);
    const bracketContainer = byId('bracket-container');
    try {
      const data = await getJson(
        `/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(year)}/draw`
      );
      if (bracketContainer) {
        renderBracket(bracketContainer, data);
      }
      state.drawLoaded = true;
      window.lucide?.createIcons?.();
    } catch (error) {
      console.error(error);
      if (bracketContainer) {
        bracketContainer.innerHTML = '<p class="text-sm text-content-faint py-6">Failed to load draw.</p>';
      }
    } finally {
      setSectionLoading('draw-loading', ['draw-content'], false);
    }
  }

  function renderForecast(data) {
    const loading = byId('forecast-loading');
    const content = byId('forecast-content');
    const empty = byId('forecast-empty');
    if (!loading || !content || !empty) return;

    loading.classList.add('hidden');

    if (!data?.has_forecast || !data?.players?.length) {
      empty.classList.remove('hidden');
      return;
    }

    const players = data.players;
    const spotlight = byId('forecast-spotlight');
    const tableBody = byId('forecast-table-body');
    const meta = byId('forecast-meta');

    function fmtPct(v) {
      if (v == null) return '—';
      return `${Math.round(v * 100)}%`;
    }

    function probBar(v) {
      const pct = v != null ? Math.round(v * 100) : 0;
      return `
        <div class="flex items-center gap-2">
          <div class="flex-1 h-1 rounded-full bg-surface-muted overflow-hidden">
            <div class="h-full rounded-full bg-teelo-lime" style="width:${pct}%"></div>
          </div>
          <span class="text-[11px] font-semibold text-content-secondary w-7 text-right shrink-0">${fmtPct(v)}</span>
        </div>`;
    }

    // Spotlight: top 3 players
    const top3 = players.slice(0, 3);
    const rest = players.slice(3);

    if (spotlight) {
      spotlight.innerHTML = top3.map((p, i) => {
        const isFav = i === 0;
        const cardBorder = isFav ? 'border-teelo-lime/40 bg-teelo-lime/5' : 'border-line-subtle bg-surface-alt/40';
        const rankBadge = isFav
          ? `<span class="inline-flex items-center gap-1 text-[10px] font-bold text-teelo-ink bg-teelo-lime rounded-full px-2 py-0.5 leading-none">
               <svg class="w-2.5 h-2.5" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
               #1
             </span>`
          : `<span class="text-[10px] font-bold text-content-faint bg-surface-muted rounded-full px-2 py-0.5">#${i + 1}</span>`;
        const roleLabel = isFav ? 'Favourite' : 'Contender';

        const rows = [
          { label: 'QF', val: p.reach_qf },
          { label: 'SF', val: p.reach_sf },
          { label: 'F', val: p.reach_f },
          { label: 'Win', val: p.win_title, bold: true },
        ].filter((r) => r.val != null);

        return `
          <div class="rounded-xl border ${cardBorder} p-4 relative overflow-hidden">
            <div class="absolute top-2 right-2">${rankBadge}</div>
            <p class="text-[10px] uppercase tracking-wider text-content-faint font-bold mb-1">${escapeHtml(roleLabel)}</p>
            <p class="text-base font-bold text-teelo-dark pr-10">${escapeHtml(p.name || '—')}</p>
            <div class="space-y-1.5 mt-3">
              ${rows.map((r) => `
                <div class="flex items-center gap-2">
                  <span class="text-[10px] ${r.bold ? 'font-bold text-teelo-dark' : 'text-content-faint'} w-7 shrink-0">${r.label}</span>
                  ${probBar(r.val)}
                </div>`).join('')}
            </div>
          </div>`;
      }).join('');
    }

    // Remaining players table
    if (tableBody) {
      tableBody.innerHTML = rest.map((p, i) => {
        const rank = i + 4;
        return `
          <tr class="hover:bg-surface-hover/50 transition-colors duration-75">
            <td class="px-4 py-2.5 text-xs font-bold text-content-faintest">${rank}</td>
            <td class="px-4 py-2.5 text-sm font-semibold text-teelo-dark">${escapeHtml(p.name || '—')}</td>
            <td class="px-4 py-2.5 text-center text-xs text-content-secondary hidden sm:table-cell">${fmtPct(p.reach_qf)}</td>
            <td class="px-4 py-2.5 text-center text-xs text-content-secondary hidden sm:table-cell">${fmtPct(p.reach_sf)}</td>
            <td class="px-4 py-2.5 text-center text-xs text-content-secondary hidden sm:table-cell">${fmtPct(p.reach_f)}</td>
            <td class="px-4 py-2.5 text-right text-sm font-bold text-teelo-dark">${fmtPct(p.win_title)}</td>
          </tr>`;
      }).join('');
    }

    // Metadata footer
    if (meta && data.forecast_run) {
      const run = data.forecast_run;
      const generatedAt = run.generated_at ? new Date(run.generated_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
      meta.innerHTML = `
        <p class="text-[11px] text-content-faint">Generated ${generatedAt} · run #${escapeHtml(String(run.id))} · <span class="text-status-success font-semibold">${escapeHtml(run.status)}</span></p>
        <p class="text-[11px] text-content-faint italic">Probabilities do not account for withdrawals or walkovers.</p>`;
    }

    content.classList.remove('hidden');
    state.forecastLoaded = true;
  }

  async function loadForecast() {
    const loading = byId('forecast-loading');
    const content = byId('forecast-content');
    loading?.classList.remove('hidden');
    content?.classList.add('hidden');
    try {
      const data = await getJson(
        `/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(year)}/forecast`
      );
      renderForecast(data);
    } catch (error) {
      console.error(error);
      renderForecast(null);
    } finally {
      loading?.classList.add('hidden');
    }
  }

  function renderEditionHistory(editions) {
    const tableBody = byId('edition-history-table-body');
    const cards = byId('edition-history-cards');
    const empty = byId('edition-history-empty');
    if (!tableBody || !cards || !empty) return;

    if (!editions.length) {
      tableBody.innerHTML = '';
      cards.innerHTML = '';
      empty.classList.remove('hidden');
      return;
    }

    empty.classList.add('hidden');
    const currentYear = Number.parseInt(year, 10);

    tableBody.innerHTML = editions
      .map((item) => {
        const isCurrent = Number(item.year) === currentYear;
        const rowClass = isCurrent ? 'bg-teelo-lime/20' : 'hover:bg-surface-hover';
        const yearCellClass = isCurrent ? 'text-teelo-dark font-bold' : 'text-content-secondary';
        const champion = item.champion || '—';
        const runnerUp = item.runner_up || '—';
        const score = item.score || '—';
        const surface = item.surface || '—';
        const championCell = renderPlayerLink(champion, item.champion_id, 'text-teelo-dark font-semibold', item.champion_url || null);
        const runnerUpCell = renderPlayerLink(runnerUp, item.runner_up_id, 'text-content-secondary', item.runner_up_url || null);

        return `
          <tr class="${rowClass}">
            <td class="px-5 py-3 ${yearCellClass}"><a href="${escapeHtml(item.url || '#')}" class="hover:underline decoration-teelo-lime decoration-2">${escapeHtml(item.year)}</a></td>
            <td class="px-5 py-3">${championCell}</td>
            <td class="px-5 py-3">${runnerUpCell}</td>
            <td class="px-5 py-3 text-content-secondary font-mono text-xs">${escapeHtml(score)}</td>
            <td class="px-5 py-3 text-sm ${surfaceClass(surface)}">${escapeHtml(surface)}</td>
          </tr>
        `;
      })
      .join('');

    cards.innerHTML = editions
      .map((item) => {
        const isCurrent = Number(item.year) === currentYear;
        const cardClass = isCurrent ? 'bg-teelo-lime/20' : 'bg-surface';
        const surface = item.surface || '—';
        const championCell = renderPlayerLink(item.champion || '—', item.champion_id, 'font-semibold text-teelo-dark', item.champion_url || null);
        const runnerUpCell = renderPlayerLink(item.runner_up || '—', item.runner_up_id, '', item.runner_up_url || null);
        return `
          <a href="${escapeHtml(item.url || '#')}" class="block px-4 py-3 ${cardClass}">
            <div class="flex items-center justify-between gap-2 mb-1.5">
              <span class="text-sm font-bold text-teelo-dark">${escapeHtml(item.year)}</span>
              <span class="text-[11px] ${surfaceClass(surface)} font-semibold">${escapeHtml(surface)}</span>
            </div>
            <div class="text-xs text-content-secondary">
              ${championCell}
              <span class="text-content-faint"> def. </span>
              ${runnerUpCell}
            </div>
            <div class="text-[11px] font-mono text-content-muted mt-1">${escapeHtml(item.score || '—')}</div>
          </a>
        `;
      })
      .join('');
  }

  async function loadEditions() {
    if (state.editionsLoaded) return;
    setSectionLoading('editions-loading', ['editions-content'], true);
    try {
      const data = await getJson(`/api/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/editions`);
      renderEditionHistory(data?.editions || []);
      state.editionsLoaded = true;
    } catch (error) {
      console.error(error);
      renderEditionHistory([]);
    } finally {
      setSectionLoading('editions-loading', ['editions-content'], false);
    }
  }

  queryAll('[data-tab]', root).forEach((button) => {
    button.addEventListener('click', () => {
      setActiveTab(button.dataset.tab || 'matches');
    });
  });

  queryAll('[data-match-status]', root).forEach((button) => {
    button.addEventListener('click', () => {
      const nextStatus = button.dataset.matchStatus || 'completed';
      if (state.matchStatus === nextStatus) return;
      state.matchStatus = nextStatus;
      state.matchPage = 1;
      queryAll('[data-match-status]', root).forEach((chip) => {
        chip.classList.toggle('active', chip === button);
      });
      loadMatches();
    });
  });

  const yearSelect = byId('tournament-year-select');
  yearSelect?.addEventListener('change', (event) => {
    const nextYear = event.target?.value;
    if (!nextYear || nextYear === year) return;
    window.location.assign(`/tournaments/${encodeURIComponent(tour)}/${encodeURIComponent(tournamentCode)}/${encodeURIComponent(nextYear)}`);
  });

  setActiveTab('matches');
  loadMatches();
  loadEditions();
  enableMatchRowNavigation();
  window.lucide?.createIcons?.();
}
