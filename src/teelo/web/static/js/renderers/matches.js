import { escapeHtml, formatNumber } from '../lib/format.js';

function playerHref(player) {
  if (!player?.id) return null;
  return player.player_url || `/players/${encodeURIComponent(String(player.id))}`;
}

/* ── Helper functions (ported from Jinja2 macros in match_rows.html) ── */

function circuitBg(tour, level) {
    if (level === 'Grand Slam') return 'bg-tour-grandslam';
    if (tour === 'ATP') return 'bg-tour-atp';
    if (tour === 'WTA') return 'bg-tour-wta';
    if (tour === 'CHALLENGER' || tour === 'Challenger') return 'bg-tour-challenger';
    if (tour === 'WTA 125' || tour === 'WTA_125' || level === 'WTA 125' || level === 'WTA_125') return 'bg-tour-250';
    if (tour === 'ITF' || (level && level.includes('ITF'))) return 'bg-tour-itf';
    return 'bg-tour-itf';
}

function circuitLabel(tour, level) {
    if (level === 'Grand Slam') return 'GSL';
    if (tour === 'CHALLENGER' || tour === 'Challenger') return 'CHL';
    if (tour === 'WTA 125' || tour === 'WTA_125' || level === 'WTA 125' || level === 'WTA_125') return '125';
    if (tour === 'ITF' || (level && level.includes('ITF'))) return 'ITF';
    if (tour === 'WTA') return 'WTA';
    if (tour === 'ATP') return 'ATP';
    return 'UNK';
}

function genderDotCls(gender, tour) {
    if (gender === 'women' || tour === 'WTA' || tour === 'WTA 125' || tour === 'WTA_125') return 'bg-gender-female';
    if (gender === 'men' || tour === 'ATP' || tour === 'CHALLENGER' || tour === 'Challenger') return 'bg-gender-male';
    return 'bg-content-faint';
}

function genderLabel(gender, tour) {
    if (gender === 'women') return 'Women';
    if (gender === 'men') return 'Men';
    if (tour === 'WTA' || tour === 'WTA 125' || tour === 'WTA_125') return 'Women';
    if (tour === 'ATP' || tour === 'CHALLENGER' || tour === 'Challenger') return 'Men';
    return 'Unknown';
}

function surfaceCls(surface) {
    if (surface === 'Hard') return 'surface-text-hard';
    if (surface === 'Clay') return 'surface-text-clay';
    if (surface === 'Grass') return 'surface-text-grass';
    if (surface === 'Indoor') return 'surface-text-indoor';
    return '';
}

function eloBadge(player) {
    if (player?.elo_pre == null) return '';
    let changeHtml = '';
    if (player.elo_change != null) {
        const cls = player.elo_change > 0 ? 'text-status-success' : (player.elo_change < 0 ? 'text-status-danger' : 'text-content-faint');
        const formatted = player.elo_change >= 0 ? `+${player.elo_change}` : `${player.elo_change}`;
        changeHtml = `<span class="font-semibold ${cls}">${formatted}</span>`;
    }
    return `<span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full bg-surface-muted text-[11px] leading-none"><span class="text-content-muted font-medium">${player.elo_pre}</span>${changeHtml}</span>`;
}

function eloCompact(player) {
    if (player?.elo_pre == null) return '';
    let changeHtml = '';
    if (player.elo_change != null) {
        const cls = player.elo_change > 0 ? 'text-status-success' : (player.elo_change < 0 ? 'text-status-danger' : 'text-content-faint');
        const formatted = player.elo_change >= 0 ? `+${player.elo_change}` : `${player.elo_change}`;
        changeHtml = `<span class="font-semibold ${cls}">${formatted}</span>`;
    }
    return `<span class="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full bg-surface-muted text-[10px] leading-none whitespace-nowrap flex-shrink-0 ml-1"><span class="text-content-muted font-medium">${player.elo_pre}</span>${changeHtml}</span>`;
}

/* ── Table rows (desktop) ── */

export function buildFallbackTableRows(matches) {
    return matches.map(m => {
        const isWinnerA = m.winner_id != null && m.winner_id === m.player_a?.id;
        const isWinnerB = m.winner_id != null && m.winner_id === m.player_b?.id;
        const playerAUrl = m.player_a?.player_url || `/players/${m.player_a?.id ?? ''}`;
        const playerBUrl = m.player_b?.player_url || `/players/${m.player_b?.id ?? ''}`;
        const scoreDisplay = m.status === 'walkover' ? 'W/O' : (m.score || 'vs');
        const matchUrl = m.match_url || `/matches/${m.id}`;

        const tour = m.tour || '';
        const level = m.tournament_level || '';
        const gender = m.gender || '';
        const surface = m.surface || '';

        // Prediction
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;

        // Tournament name — link or span
        const tournamentName = escapeHtml(m.tournament_name || 'Unknown');
        const tournamentNameHtml = m.tournament_url
            ? `<a href="${escapeHtml(m.tournament_url)}" class="text-sm font-semibold text-teelo-dark truncate block hover:underline decoration-teelo-lime decoration-2" title="${tournamentName}" onclick="event.stopPropagation()">${tournamentName}</a>`
            : `<span class="text-sm font-semibold text-teelo-dark truncate block" title="${tournamentName}">${tournamentName}</span>`;

        // Player A styling
        const playerACls = isWinnerA ? 'text-teelo-dark font-bold' : 'text-content-faint';
        const playerACheck = isWinnerA ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '';

        // Player B styling
        const playerBCls = isWinnerB ? 'text-teelo-dark font-bold' : 'text-content-faint';
        const playerBCheck = isWinnerB ? '<i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>' : '';

        // Score styling
        const scoreCls = scoreDisplay === 'vs' ? 'text-content-faint italic' : 'text-teelo-dark font-semibold';

        // Level context
        const levelCtx = `${escapeHtml(genderLabel(gender, tour))} \u2022 ${escapeHtml(level || 'Unknown')}`;

        return `<tr class="hover:bg-surface-hover/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime cursor-pointer" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
    <td class="px-5 py-3">
        <div class="flex items-center gap-2">
            <span class="${circuitBg(tour, level)} text-content-inverse text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">${circuitLabel(tour, level)}</span>
            <div class="min-w-0">
                ${tournamentNameHtml}
                <span class="text-xs text-content-faint inline-flex items-center gap-1.5 flex-wrap">
                    <span>${escapeHtml(m.round || '')}</span>
                    <span>\u00b7</span>
                    <span class="inline-flex items-center gap-1">
                        <span class="w-1.5 h-1.5 rounded-full ${genderDotCls(gender, tour)}"></span>
                        <span class="text-content-muted font-medium">${levelCtx}</span>
                    </span>
                    <span>\u00b7</span>
                    <span class="${surfaceCls(surface)}">${escapeHtml(surface)}</span>
                </span>
            </div>
        </div>
    </td>
    <td class="px-5 py-3 text-right">
        <div class="flex items-center justify-end gap-2">
            <a href="${escapeHtml(playerAUrl)}" class="text-sm hover:underline decoration-teelo-lime decoration-2 ${playerACls}" onclick="event.stopPropagation()">${escapeHtml(m.player_a?.name || '')}</a>
            ${eloBadge(m.player_a)}
            ${playerACheck}
        </div>
    </td>
    <td class="px-3 py-3 text-center">
        ${predA != null ? `<div class="flex flex-col items-center" style="min-width:110px;">
            ${scoreDisplay !== 'vs' ? `<span class="text-teelo-dark font-semibold text-[11px] mb-1">${escapeHtml(scoreDisplay)}</span>` : ''}
            <div class="flex justify-between w-full" style="font-size:10px;margin-bottom:2px;">
                <span class="${predA >= predB ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predA}%</span>
                <span class="${predB > predA ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predB}%</span>
            </div>
            <div class="flex w-full overflow-hidden" style="height:6px;border-radius:3px;">
                <div class="bg-teelo-lime" style="width:${predA}%;border-radius:3px 0 0 3px;"></div>
                <div class="bg-surface-muted" style="width:${predB}%;border-radius:0 3px 3px 0;"></div>
            </div>
        </div>` : `<span class="inline-block px-2.5 py-1 bg-surface-alt rounded-md text-xs font-mono whitespace-nowrap group-hover:bg-teelo-lime/10 transition-colors ${scoreCls}">${escapeHtml(scoreDisplay)}</span>`}
    </td>
    <td class="px-5 py-3">
        <div class="flex items-center gap-2">
            ${playerBCheck}
            <a href="${escapeHtml(playerBUrl)}" class="text-sm hover:underline decoration-teelo-lime decoration-2 ${playerBCls}" onclick="event.stopPropagation()">${escapeHtml(m.player_b?.name || '')}</a>
            ${eloBadge(m.player_b)}
        </div>
    </td>
    <td class="px-5 py-3 text-right"><span class="text-xs text-content-faint whitespace-nowrap">${escapeHtml(m.match_date_display || '')}</span></td>
    <td class="pr-3 py-3 w-8"><a href="${escapeHtml(matchUrl)}" class="text-content-faintest group-hover:text-teelo-lime transition-colors" onclick="event.stopPropagation()" aria-label="View match details"><i data-lucide="chevron-right" class="w-4 h-4"></i></a></td>
</tr>`;
    }).join('');
}

/* ── Cards (mobile) ── */

export function buildFallbackCards(matches) {
    return matches.map(m => {
        const isWinnerA = m.winner_id != null && m.winner_id === m.player_a?.id;
        const isWinnerB = m.winner_id != null && m.winner_id === m.player_b?.id;
        const hasWinner = isWinnerA || isWinnerB;
        const playerAUrl = m.player_a?.player_url || `/players/${m.player_a?.id ?? ''}`;
        const playerBUrl = m.player_b?.player_url || `/players/${m.player_b?.id ?? ''}`;
        const scoreDisplay = m.status === 'walkover' ? 'W/O' : (m.score || 'vs');
        const matchUrl = m.match_url || `/matches/${m.id}`;

        const tour = m.tour || '';
        const level = m.tournament_level || '';
        const gender = m.gender || '';
        const surface = m.surface || '';

        // Prediction
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;
        const predHtml = predA != null
            ? `<div class="flex items-center gap-2 text-[11px] mb-2">
                 <span class="${predA >= predB ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predA}%</span>
                 <div class="flex-1 h-1 rounded-full bg-surface-muted overflow-hidden"><div class="h-full bg-teelo-lime rounded-full" style="width:${predA}%"></div></div>
                 <span class="${predB > predA ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predB}%</span>
               </div>`
            : '';

        // Tournament name — link or span
        const tournamentName = escapeHtml(m.tournament_name || 'Unknown');
        const tournamentNameHtml = m.tournament_url
            ? `<a href="${escapeHtml(m.tournament_url)}" class="text-[13px] font-semibold text-teelo-dark truncate hover:underline decoration-teelo-lime decoration-2" onclick="event.stopPropagation()">${tournamentName}</a>`
            : `<span class="text-[13px] font-semibold text-teelo-dark truncate">${tournamentName}</span>`;

        // Player styling
        const playerACls = isWinnerA ? 'font-bold text-teelo-dark' : (hasWinner ? 'text-content-faint' : 'text-teelo-dark font-medium');
        const playerBCls = isWinnerB ? 'font-bold text-teelo-dark' : (hasWinner ? 'text-content-faint' : 'text-teelo-dark font-medium');

        // Winner accent bars
        const accentA = isWinnerA ? 'bg-teelo-lime' : 'bg-transparent';
        const accentB = isWinnerB ? 'bg-teelo-lime' : 'bg-transparent';

        // Score styling
        let scoreCls;
        if (scoreDisplay === 'vs') {
            scoreCls = 'text-content-faintest italic font-sans';
        } else if (scoreDisplay === 'W/O') {
            scoreCls = 'text-content-faint font-sans font-medium';
        } else {
            scoreCls = 'text-teelo-dark font-semibold';
        }

        return `<div class="px-4 py-3 border-b border-line-subtle last:border-b-0 cursor-pointer hover:bg-surface-hover/50 transition-colors" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
    <div class="flex items-center gap-2 mb-0.5">
        <span class="${circuitBg(tour, level)} text-content-inverse text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">${circuitLabel(tour, level)}</span>
        ${tournamentNameHtml}
        <a href="${escapeHtml(matchUrl)}" class="ml-auto text-content-faintest flex-shrink-0" onclick="event.stopPropagation()" aria-label="View match details"><i data-lucide="chevron-right" class="w-4 h-4"></i></a>
    </div>
    <div class="flex items-center gap-1.5 text-[11px] text-content-faint mb-2">
        <span class="font-medium">${escapeHtml(m.round || '')}</span>
        <span class="text-content-faintest">\u00b7</span>
        <span class="w-1.5 h-1.5 rounded-full ${genderDotCls(gender, tour)} flex-shrink-0"></span>
        <span class="${surfaceCls(surface)} font-medium">${escapeHtml(surface)}</span>
        <span class="text-content-faintest">\u00b7</span>
        <span>${escapeHtml(m.match_date_display || '')}</span>
    </div>
    ${predHtml}
    <div class="flex items-center">
        <div class="flex-1 min-w-0 space-y-1">
            <div class="flex items-center gap-1.5">
                <div class="w-0.5 h-4 rounded-full ${accentA} flex-shrink-0"></div>
                <a href="${escapeHtml(playerAUrl)}" class="text-[13px] truncate hover:underline decoration-teelo-lime decoration-2 ${playerACls}" onclick="event.stopPropagation()">${escapeHtml(m.player_a?.name || '')}</a>
                ${eloCompact(m.player_a)}
            </div>
            <div class="flex items-center gap-1.5">
                <div class="w-0.5 h-4 rounded-full ${accentB} flex-shrink-0"></div>
                <a href="${escapeHtml(playerBUrl)}" class="text-[13px] truncate hover:underline decoration-teelo-lime decoration-2 ${playerBCls}" onclick="event.stopPropagation()">${escapeHtml(m.player_b?.name || '')}</a>
                ${eloCompact(m.player_b)}
            </div>
        </div>
        <span class="text-xs font-mono whitespace-nowrap flex-shrink-0 ml-3 ${scoreCls}">${escapeHtml(scoreDisplay)}</span>
    </div>
</div>`;
    }).join('');
}

export function renderMatchesView(els, data, append) {
  const matches = data.matches || [];

  // Hide skeleton and error state on any render
  const skeleton = document.getElementById('skeleton-loading');
  if (skeleton) skeleton.classList.add('hidden');
  const errorState = document.getElementById('error-state');
  if (errorState) errorState.classList.add('hidden');

  if (!append && matches.length === 0) {
    els.emptyState.classList.remove('hidden');
    els.tableBody.innerHTML = '';
    els.cardsContainer.innerHTML = '';
    if (els.scrollSentinel) els.scrollSentinel.classList.add('hidden');
    els.resultsCount.textContent = '0 matches';
    return;
  }

  els.emptyState.classList.add('hidden');

  // Use server-rendered HTML if available, otherwise render client-side
  const tableHtml = data.table_rows_html || buildFallbackTableRows(matches);
  const cardsHtml = data.cards_html || buildFallbackCards(matches);

  if (append) {
    els.tableBody.insertAdjacentHTML('beforeend', tableHtml);
    els.cardsContainer.insertAdjacentHTML('beforeend', cardsHtml);
  } else {
    els.tableBody.innerHTML = tableHtml;
    els.cardsContainer.innerHTML = cardsHtml;
  }

  // Update results count
  if (data.total != null) {
    els.resultsCount.textContent = `${formatNumber(data.total)} match${data.total === 1 ? '' : 'es'}`;
  } else {
    // When total is unknown (LIMIT+1 mode), show approximate count
    const currentCount = els.tableBody.querySelectorAll('tr').length;
    els.resultsCount.textContent = data.has_more ? `${formatNumber(currentCount)}+ matches` : `${formatNumber(currentCount)} match${currentCount === 1 ? '' : 'es'}`;
  }

  // Toggle sentinel visibility
  if (els.scrollSentinel) {
    els.scrollSentinel.classList.toggle('hidden', !data.has_more);
  }

  // Scope icon hydration to new content only
  if (window.lucide?.createIcons) {
    const containers = [els.tableBody?.parentElement, els.cardsContainer].filter(Boolean);
    if (containers.length) {
      try { window.lucide.createIcons({ nodes: containers }); } catch { window.lucide.createIcons(); }
    }
  }
}

export function renderSummaryTags(els, tags, onRemove) {
  if (!tags.length) {
    els.filterSummary.classList.add('hidden');
    els.filterTags.innerHTML = '';
    return;
  }
  els.filterSummary.classList.remove('hidden');
  els.filterTags.innerHTML = tags.map((tag) => (
    `<span class="filter-summary-tag">${escapeHtml(tag.label)} <button data-remove="${escapeHtml(tag.removeKey)}">&times;</button></span>`
  )).join('');
  els.filterTags.querySelectorAll('button[data-remove]').forEach((btn) => {
    btn.addEventListener('click', () => onRemove(btn.dataset.remove));
  });
}
