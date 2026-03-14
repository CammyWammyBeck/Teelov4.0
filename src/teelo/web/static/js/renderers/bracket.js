import { escapeHtml, slugifyName } from '../lib/format.js';

const cleanupMap = new WeakMap();

function expectedMatchesForRound(roundCode, fallbackCount) {
  if (!roundCode) return fallbackCount;
  if (roundCode === 'F') return 1;
  if (roundCode === 'SF') return 2;
  if (roundCode === 'QF') return 4;
  if (roundCode === 'R16') return 8;
  if (roundCode === 'R32') return 16;
  if (roundCode === 'R64') return 32;
  if (roundCode === 'R128') return 64;
  return fallbackCount;
}

function formatSeed(seed) {
  if (seed == null || seed === '') return '';
  return `[${seed}]`;
}

function playerRowHtml(player, isWinner) {
  const label = player?.name || 'TBD';
  const seed = formatSeed(player?.seed);
  const hasPlayerLink = Number.isFinite(Number(player?.id));
  const playerUrl = player?.player_url || `/players/${encodeURIComponent(String(player?.id || ''))}/${slugifyName(label)}`;
  const playerLabel = hasPlayerLink
    ? `<a href="${escapeHtml(playerUrl)}" class="truncate ${isWinner ? 'font-semibold text-teelo-dark' : 'text-gray-600'} hover:underline decoration-teelo-lime decoration-2">${escapeHtml(label)}</a>`
    : `<span class="truncate ${isWinner ? 'font-semibold text-teelo-dark' : 'text-gray-600'}">${escapeHtml(label)}</span>`;
  return `
    <div class="flex items-center gap-1.5 min-w-0 ${isWinner ? 'border-l-2 border-teelo-lime pl-1.5' : 'pl-[7px]'}">
      <span class="text-[11px] text-gray-400 shrink-0 min-w-[1.75rem]">${escapeHtml(seed)}</span>
      ${playerLabel}
    </div>
  `;
}

function getRoundMatchMap(round) {
  const map = new Map();
  const matches = Array.isArray(round?.matches) ? round.matches : [];
  for (const match of matches) {
    const pos = Number(match?.draw_position);
    if (!Number.isFinite(pos) || pos <= 0) continue;
    map.set(pos, match);
  }
  return map;
}

function buildMatchCard(match, visibleRoundIndex, position, isPlaceholder = false) {
  const winnerId = match?.winner_id ?? null;
  const playerA = match?.player_a || { name: 'TBD', seed: null, id: null };
  const playerB = match?.player_b || { name: 'TBD', seed: null, id: null };
  const topWinner = winnerId != null && playerA?.id != null && winnerId === playerA.id;
  const bottomWinner = winnerId != null && playerB?.id != null && winnerId === playerB.id;
  const scoreText = match?.status === 'completed' ? (match.score || '—') : (match?.score || '');

  const card = document.createElement('div');
  card.className = 'teelo-bracket-match bg-white border border-gray-200 rounded-lg p-2.5 text-sm shadow-sm';
  if (isPlaceholder) {
    card.classList.add('teelo-bracket-placeholder', 'bg-gray-50/80');
  }
  card.dataset.roundIndex = String(visibleRoundIndex);
  card.dataset.position = String(position);

  card.innerHTML = `
    <div class="space-y-1">
      ${playerRowHtml(playerA, topWinner)}
      <div class="text-center text-[11px] font-mono text-gray-500 min-h-[1rem]">${escapeHtml(scoreText || '')}</div>
      ${playerRowHtml(playerB, bottomWinner)}
    </div>
  `;

  return card;
}

function createDebounced(fn, delayMs) {
  let timeoutId = null;
  return () => {
    if (timeoutId) window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => {
      timeoutId = null;
      fn();
    }, delayMs);
  };
}

function drawConnectors(content, svg, isLastRoundTrailing = false) {
  const rootRect = content.getBoundingClientRect();
  const width = Math.max(content.scrollWidth, Math.ceil(rootRect.width));
  const height = Math.max(content.scrollHeight, Math.ceil(rootRect.height));

  svg.setAttribute('width', String(width));
  svg.setAttribute('height', String(height));
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.innerHTML = '';

  const cards = Array.from(content.querySelectorAll('.teelo-bracket-match'));
  const byRound = new Map();
  for (const card of cards) {
    const roundIndex = Number(card.dataset.roundIndex);
    const position = Number(card.dataset.position);
    if (!Number.isFinite(roundIndex) || !Number.isFinite(position)) continue;
    if (!byRound.has(roundIndex)) byRound.set(roundIndex, new Map());
    byRound.get(roundIndex).set(position, card);
  }

  const roundKeys = Array.from(byRound.keys()).sort((a, b) => a - b);
  const maxRoundIndex = roundKeys.length ? roundKeys[roundKeys.length - 1] : -1;

  for (const roundIndex of roundKeys) {
    const sourceMap = byRound.get(roundIndex);
    const targetMap = byRound.get(roundIndex + 1);

    if (targetMap) {
      // Normal connectors between two visible rounds
      for (const [position, sourceCard] of sourceMap.entries()) {
        const targetCard = targetMap.get(Math.ceil(position / 2));
        if (!targetCard) continue;

        const sourceRect = sourceCard.getBoundingClientRect();
        const targetRect = targetCard.getBoundingClientRect();
        const x1 = sourceRect.right - rootRect.left;
        const y1 = sourceRect.top + sourceRect.height / 2 - rootRect.top;
        const x2 = targetRect.left - rootRect.left;
        const y2 = targetRect.top + targetRect.height / 2 - rootRect.top;
        const midX = x1 + (x2 - x1) * 0.48;

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', `M ${x1} ${y1} L ${midX} ${y1} L ${midX} ${y2} L ${x2} ${y2}`);
        path.setAttribute('fill', 'none');
        path.setAttribute('stroke', '#D1D5DB');
        path.setAttribute('stroke-width', '1.5');
        path.setAttribute('stroke-linecap', 'round');
        path.setAttribute('stroke-linejoin', 'round');
        svg.appendChild(path);
      }
    } else if (roundIndex === maxRoundIndex && isLastRoundTrailing) {
      // Trailing connectors off the right edge for the last visible round
      // Group cards into pairs that would feed the same next-round match
      const positions = Array.from(sourceMap.keys()).sort((a, b) => a - b);
      for (let p = 0; p < positions.length; p += 2) {
        const topCard = sourceMap.get(positions[p]);
        const bottomCard = positions[p + 1] != null ? sourceMap.get(positions[p + 1]) : null;
        if (!topCard) continue;

        const topRect = topCard.getBoundingClientRect();
        const x1 = topRect.right - rootRect.left;
        const y1 = topRect.top + topRect.height / 2 - rootRect.top;
        const trailX = width; // right edge of the bracket area
        const midX = x1 + (trailX - x1) * 0.48;

        if (bottomCard) {
          const bottomRect = bottomCard.getBoundingClientRect();
          const y2 = bottomRect.top + bottomRect.height / 2 - rootRect.top;
          const mergeY = (y1 + y2) / 2;

          // Top match line going right then to merge point
          const p1 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          p1.setAttribute('d', `M ${x1} ${y1} L ${midX} ${y1} L ${midX} ${mergeY}`);
          p1.setAttribute('fill', 'none');
          p1.setAttribute('stroke', '#D1D5DB');
          p1.setAttribute('stroke-width', '1.5');
          p1.setAttribute('stroke-linecap', 'round');
          p1.setAttribute('stroke-linejoin', 'round');
          svg.appendChild(p1);

          // Bottom match line going right then up to merge point
          const bx1 = bottomRect.right - rootRect.left;
          const p2 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          p2.setAttribute('d', `M ${bx1} ${y2} L ${midX} ${y2} L ${midX} ${mergeY}`);
          p2.setAttribute('fill', 'none');
          p2.setAttribute('stroke', '#D1D5DB');
          p2.setAttribute('stroke-width', '1.5');
          p2.setAttribute('stroke-linecap', 'round');
          p2.setAttribute('stroke-linejoin', 'round');
          svg.appendChild(p2);
        } else {
          // Single match (odd count) — just a short trailing line
          const p1 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          p1.setAttribute('d', `M ${x1} ${y1} L ${midX} ${y1}`);
          p1.setAttribute('fill', 'none');
          p1.setAttribute('stroke', '#D1D5DB');
          p1.setAttribute('stroke-width', '1.5');
          p1.setAttribute('stroke-linecap', 'round');
          svg.appendChild(p1);
        }
      }
    }
  }
}

function cleanupContainer(container) {
  const cleanup = cleanupMap.get(container);
  if (!cleanup) return;
  cleanup();
  cleanupMap.delete(container);
}

function getVisibleWindow(rounds, activeIndex) {
  const isMobile = window.innerWidth < 640;
  const windowSize = isMobile ? 1 : 3;
  const half = Math.floor(windowSize / 2);
  let start = activeIndex - half;
  let end = start + windowSize - 1;

  if (start < 0) { start = 0; end = Math.min(windowSize - 1, rounds.length - 1); }
  if (end >= rounds.length) { end = rounds.length - 1; start = Math.max(0, end - windowSize + 1); }

  return { start, end };
}

function renderBracketGrid(gridEl, svgEl, rounds, activeIndex) {
  // Determine visible rounds
  const { start, end } = getVisibleWindow(rounds, activeIndex);
  const visibleRounds = rounds.slice(start, end + 1);

  // Compute firstRoundCount (leftmost visible round's expected count)
  const firstRound = visibleRounds[0];
  const firstMatchMap = getRoundMatchMap(firstRound);
  const firstFallback = firstMatchMap.size > 0 ? Math.max(...firstMatchMap.keys()) : (firstRound?.matches?.length || 1);
  const firstRoundCount = expectedMatchesForRound(firstRound?.round, firstFallback);

  // Set up grid columns
  gridEl.style.display = 'grid';
  gridEl.style.gridTemplateColumns = `repeat(${visibleRounds.length}, 1fr)`;
  gridEl.style.gap = '32px';
  gridEl.style.alignItems = 'stretch';
  gridEl.style.minHeight = `${firstRoundCount * 90}px`;
  gridEl.innerHTML = '';

  visibleRounds.forEach((round, vi) => {
    const matchMap = getRoundMatchMap(round);
    const fallbackCount = matchMap.size > 0 ? Math.max(...matchMap.keys()) : (round?.matches?.length || 1);
    const expectedCount = expectedMatchesForRound(round?.round, fallbackCount);

    // Each match spans (firstRoundCount / expectedCount) grid rows so it
    // centers between its two feeder matches from the previous round.
    const stride = Math.max(1, Math.round(firstRoundCount / expectedCount));

    const column = document.createElement('div');
    column.className = 'teelo-bracket-round';
    // Use CSS grid within each column for the matches
    column.style.display = 'grid';
    column.style.gridTemplateRows = `auto repeat(${firstRoundCount}, 1fr)`;
    column.style.gap = '0';

    // Header (row 1 of the inner grid)
    const header = document.createElement('div');
    header.className = 'teelo-bracket-round-header text-xs uppercase tracking-wider text-gray-400 font-bold text-center';
    header.textContent = round?.label || round?.round || '';
    column.appendChild(header);

    // Matches
    for (let position = 1; position <= expectedCount; position++) {
      const match = matchMap.get(position);
      const card = buildMatchCard(match, vi, position, !match);

      // +2: 1 for 1-indexed grid rows, 1 for the header row
      const rowStart = (position - 1) * stride + 2;
      card.style.gridRow = `${rowStart} / span ${stride}`;
      card.style.alignSelf = 'center';

      column.appendChild(card);
    }

    gridEl.appendChild(column);
  });

  // Trailing connectors when the last visible round isn't the last round in the data
  const hasTrailing = end < rounds.length - 1;

  // Resize SVG to cover grid area
  window.requestAnimationFrame(() => {
    drawConnectors(gridEl, svgEl, hasTrailing);
  });
}

function renderNav(navEl, rounds, activeIndex, onSelect) {
  navEl.innerHTML = '';
  const currentWindow = getVisibleWindow(rounds, activeIndex);
  rounds.forEach((round, i) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = round?.round || round?.label || String(i + 1);
    btn.className = 'px-3 py-1.5 text-xs font-semibold rounded-full border select-none transition-all duration-150 whitespace-nowrap';

    const isActive = (i === activeIndex);
    const isVisible = !isActive && (i >= currentWindow.start && i <= currentWindow.end);
    const candidateWindow = getVisibleWindow(rounds, i);
    const isNoOp = !isActive && (
      candidateWindow.start === currentWindow.start &&
      candidateWindow.end === currentWindow.end
    );

    if (isActive) {
      btn.style.backgroundColor = '#ccff00';
      btn.style.color = '#1a1a1a';
      btn.style.borderColor = '#ccff00';
      btn.classList.add('scale-[1.02]', 'shadow-sm', 'cursor-pointer');
    } else if (isNoOp) {
      btn.classList.add('border-gray-200', 'text-gray-500', 'bg-white');
      btn.style.opacity = '0.4';
      btn.style.cursor = 'default';
    } else if (isVisible) {
      btn.style.backgroundColor = '#f0f9d6';
      btn.style.color = '#1a1a1a';
      btn.style.borderColor = '#e2f0b0';
      btn.classList.add('cursor-pointer');
    } else {
      btn.classList.add('border-gray-200', 'text-gray-500', 'bg-white', 'cursor-pointer');
    }

    if (!isNoOp) {
      btn.addEventListener('click', () => onSelect(i));
    }
    navEl.appendChild(btn);
  });
}

export function renderBracket(container, payload) {
  if (!container) return;
  cleanupContainer(container);

  const rounds = Array.isArray(payload?.rounds) ? payload.rounds : [];
  if (!payload?.has_draw || !rounds.length) {
    container.innerHTML = '<p class="text-sm text-gray-400 py-6">Draw is not available for this edition.</p>';
    return;
  }

  // Default activeIndex: second-to-last round (SF position), so last 3 rounds are visible
  let activeIndex = Math.max(0, rounds.length - 2);

  // Nav bar
  const nav = document.createElement('div');
  nav.className = 'teelo-bracket-nav flex flex-wrap items-center gap-2 mb-4';

  // Bracket wrapper (relative for SVG overlay)
  const wrapper = document.createElement('div');
  wrapper.className = 'teelo-bracket-content relative';
  wrapper.style.overflowX = 'auto';

  // SVG connector layer
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.classList.add('teelo-bracket-connectors');
  svg.style.position = 'absolute';
  svg.style.inset = '0';
  svg.style.pointerEvents = 'none';
  svg.style.zIndex = '0';
  wrapper.appendChild(svg);

  // Grid container (inside wrapper, above SVG)
  const grid = document.createElement('div');
  grid.className = 'teelo-bracket-grid relative z-[1]';
  wrapper.appendChild(grid);

  function selectRound(index) {
    activeIndex = index;
    renderNav(nav, rounds, activeIndex, selectRound);
    // Fade transition: need double-rAF so browser paints opacity:0 before we set 1
    grid.style.transition = 'none';
    grid.style.opacity = '0';
    window.requestAnimationFrame(() => {
      renderBracketGrid(grid, svg, rounds, activeIndex);
      window.requestAnimationFrame(() => {
        grid.style.transition = 'opacity 150ms';
        grid.style.opacity = '1';
      });
    });
  }

  container.innerHTML = '';
  container.appendChild(nav);
  container.appendChild(wrapper);

  // Initial render
  renderNav(nav, rounds, activeIndex, selectRound);
  renderBracketGrid(grid, svg, rounds, activeIndex);

  // Resize handling
  const debouncedRedraw = createDebounced(() => {
    renderNav(nav, rounds, activeIndex, selectRound);
    renderBracketGrid(grid, svg, rounds, activeIndex);
  }, 150);

  window.addEventListener('resize', debouncedRedraw);

  cleanupMap.set(container, () => {
    window.removeEventListener('resize', debouncedRedraw);
  });
}
