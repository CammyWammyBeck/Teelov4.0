import { escapeHtml, formatNumber } from '../lib/format.js';
import { hydrateMatchTimes, formatLocalTime } from '../lib/time.js';

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

/* ── Mini scoreboard (shared between table rows and cards) ── */

function buildMiniScoreboard(m, isWinnerA, isWinnerB, hasWinner, predA, predB, playerAUrl, playerBUrl) {
    const scores = m.score_structured;
    const hasScores = Array.isArray(scores) && scores.length > 0;
    const isWalkover = m.status === 'walkover';

    // Nationality helper
    const natHtml = (player) => {
        const ioc = player?.nationality_ioc;
        if (!ioc) return '';
        return `<span class="text-[9px] text-content-faint font-medium flex-shrink-0 w-6 text-center">${escapeHtml(ioc)}</span>`;
    };

    // Score cells for a player row (no ml-auto — parent handles alignment)
    const scoreCells = (scores, key, isWinner, otherKey) => {
        if (!hasScores) return '';
        const cells = scores.map(s => {
            const bold = isWinner && s[key] > s[otherKey];
            const cls = bold ? 'font-bold text-teelo-dark' : (hasWinner ? 'text-content-muted' : 'font-bold text-teelo-dark');
            return `<span class="w-5 text-center text-[11px] ${cls}">${s[key]}</span>`;
        }).join('');
        return `<div class="flex items-center gap-0 font-mono">${cells}</div>`;
    };

    // Prediction percentage for a row (fixed width for alignment)
    const predHtml = (pct, isLeading) => {
        if (pct == null) return '';
        const cls = isLeading ? 'font-semibold text-teelo-dark' : 'text-content-faint';
        return `<span class="w-9 text-right flex-shrink-0 text-[10px] ${cls}">${pct}%</span>`;
    };

    // Player row builder — split into left (flex-1, truncates) and right (fixed, aligned)
    const playerRow = (player, url, isWinner, scoreKey, otherKey, pct, isLeading, matchId, playerKey) => {
        const winnerIcon = isWinner
            ? '<i data-lucide="trophy" class="w-3 h-3 text-teelo-lime flex-shrink-0"></i>'
            : '<span class="w-3 flex-shrink-0"></span>';
        const nameCls = isWinner ? 'font-bold text-teelo-dark' : (hasWinner ? 'text-content-faint' : 'text-teelo-dark font-medium');
        const bgFill = pct != null
            ? `<div class="absolute inset-y-0 left-0 bg-teelo-lime/10" style="width:${pct}%"></div>`
            : '';
        const bettingHtml = (pct != null && matchId != null)
            ? `<div class="betting-section hidden flex flex-col items-end gap-0.5 ml-2 flex-shrink-0">
                <div class="flex items-center gap-0.5">
                    <span class="text-[10px] text-content-faint">$</span>
                    <input type="number" step="0.01" min="1" class="betting-odds-input w-14 px-1 py-0.5 text-[11px] border border-line rounded text-center bg-surface focus:outline-none focus:ring-1 focus:ring-teelo-lime focus:border-teelo-lime" data-match-id="${matchId}" data-player="${playerKey}" placeholder="Odds" onclick="event.stopPropagation()">
                </div>
                <span class="betting-ev text-[10px] text-content-faint" data-match-id="${matchId}" data-ev-player="${playerKey}"></span>
            </div>`
            : '';
        return `<div class="relative overflow-hidden rounded-sm">
            ${bgFill}
            <div class="relative flex items-center gap-1.5 min-w-0 py-0.5">
                <div class="flex items-center gap-1.5 min-w-0 flex-1">
                    ${winnerIcon}
                    ${natHtml(player)}
                    <a href="${escapeHtml(url)}" class="text-[11px] truncate hover:underline decoration-teelo-lime decoration-2 ${nameCls}" onclick="event.stopPropagation()">${escapeHtml(player?.name || '')}</a>
                    ${eloCompact(player)}
                </div>
                <div class="flex items-center flex-shrink-0">
                    ${scoreCells(scores, scoreKey, isWinner, otherKey)}
                    ${predHtml(pct, isLeading)}
                    ${bettingHtml}
                </div>
            </div>
        </div>`;
    };

    // Footer (walkover or vs)
    let footerHtml = '';
    if (isWalkover) {
        footerHtml = '<div class="text-center mt-0.5"><span class="text-[10px] text-content-faint font-medium">W/O</span></div>';
    } else if (!hasScores && predA == null) {
        footerHtml = '<div class="text-center mt-0.5"><span class="text-[10px] text-content-faintest italic">vs</span></div>';
    }

    const predALeading = predA != null && predA >= predB;
    const predBLeading = predB != null && predB > predA;

    return playerRow(m.player_a, playerAUrl, isWinnerA, 'a', 'b', predA, predALeading, m.id, 'a')
        + '<div style="height:2px"></div>'
        + playerRow(m.player_b, playerBUrl, isWinnerB, 'b', 'a', predB, predBLeading, m.id, 'b')
        + footerHtml;
}


/* ── Table rows (desktop) ── */

export function buildFallbackTableRows(matches) {
    return matches.map(m => {
        const isWinnerA = m.winner_id != null && m.winner_id === m.player_a?.id;
        const isWinnerB = m.winner_id != null && m.winner_id === m.player_b?.id;
        const hasWinner = isWinnerA || isWinnerB;
        const playerAUrl = m.player_a?.player_url || `/players/${m.player_a?.id ?? ''}`;
        const playerBUrl = m.player_b?.player_url || `/players/${m.player_b?.id ?? ''}`;
        const matchUrl = m.match_url || `/matches/${m.id}`;
        const _ft = formatLocalTime(m.match_datetime_utc);
        const dateText = _ft ? _ft.dateLabel : (m.match_date_display || '');
        const timeText = _ft ? _ft.timePart : '';

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

        // Level context
        const levelCtx = `${escapeHtml(genderLabel(gender, tour))} \u2022 ${escapeHtml(level || 'Unknown')}`;

        // Mini scoreboard
        const scoreboardHtml = buildMiniScoreboard(m, isWinnerA, isWinnerB, hasWinner, predA, predB, playerAUrl, playerBUrl);

        return `<tr class="hover:bg-surface-hover/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime cursor-pointer" data-match-url="${escapeHtml(matchUrl)}"${predA != null ? ` data-prediction-a="${m.prediction_a}"` : ''} role="link" tabindex="0">
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
    <td class="px-3 py-2" style="min-width:220px;">
        ${scoreboardHtml}
    </td>
    <td class="px-5 py-3 text-right">
        <span class="text-xs text-content-faint whitespace-nowrap"${m.match_datetime_utc ? ` data-utc-date="${escapeHtml(m.match_datetime_utc)}"` : ''}>${escapeHtml(dateText)}</span>
        ${m.has_exact_time && m.match_datetime_utc ? `<span class="block text-[11px] text-content-faint whitespace-nowrap" data-utc-time="${escapeHtml(m.match_datetime_utc)}">${escapeHtml(timeText)}</span>` : (!m.has_exact_time && (!m.score || m.score === 'vs') ? '<span class="block text-[11px] text-content-faint italic whitespace-nowrap">Not yet scheduled</span>' : '')}
    </td>
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
        const matchUrl = m.match_url || `/matches/${m.id}`;
        const _ft = formatLocalTime(m.match_datetime_utc);
        const dateText = _ft ? _ft.dateLabel : (m.match_date_display || '');
        const timeText = _ft ? _ft.timePart : '';

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
            ? `<a href="${escapeHtml(m.tournament_url)}" class="text-[13px] font-semibold text-teelo-dark truncate hover:underline decoration-teelo-lime decoration-2" onclick="event.stopPropagation()">${tournamentName}</a>`
            : `<span class="text-[13px] font-semibold text-teelo-dark truncate">${tournamentName}</span>`;

        // Mini scoreboard
        const scoreboardHtml = buildMiniScoreboard(m, isWinnerA, isWinnerB, hasWinner, predA, predB, playerAUrl, playerBUrl);

        return `<div class="px-4 py-3 border-b border-line-subtle last:border-b-0 cursor-pointer hover:bg-surface-hover/50 transition-colors" data-match-url="${escapeHtml(matchUrl)}"${predA != null ? ` data-prediction-a="${m.prediction_a}"` : ''} role="link" tabindex="0">
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
        <span${m.match_datetime_utc ? ` data-utc-date="${escapeHtml(m.match_datetime_utc)}"` : ''}>${escapeHtml(dateText)}</span>
        ${m.has_exact_time && m.match_datetime_utc ? `<span data-utc-time="${escapeHtml(m.match_datetime_utc)}">${escapeHtml(timeText)}</span>` : (!m.has_exact_time && (!m.score || m.score === 'vs') ? '<span class="italic">Not yet scheduled</span>' : '')}
    </div>
    ${scoreboardHtml}
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
  hydrateMatchTimes(els.tableBody);
  hydrateMatchTimes(els.cardsContainer);
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
