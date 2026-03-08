import { escapeHtml, slugifyName } from '../lib/format.js';

const BASE_MATCH_HEIGHT = 70;
const BASE_GAP = 14;
const HEADER_HEIGHT = 32;
const COLUMN_WIDTH = 220;

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

function buildMatchCard(match, roundIndex, position, topPx, isPlaceholder = false) {
  const winnerId = match?.winner_id ?? null;
  const playerA = match?.player_a || { name: 'TBD', seed: null, id: null };
  const playerB = match?.player_b || { name: 'TBD', seed: null, id: null };
  const topWinner = winnerId != null && playerA?.id != null && winnerId === playerA.id;
  const bottomWinner = winnerId != null && playerB?.id != null && winnerId === playerB.id;
  const scoreText = match?.status === 'completed' ? (match.score || '—') : (match?.score || '');

  const card = document.createElement('div');
  card.className = 'teelo-bracket-match bg-white border border-gray-200 rounded-lg p-2 text-sm shadow-sm';
  if (isPlaceholder) {
    card.classList.add('teelo-bracket-placeholder', 'bg-gray-50/80');
  }
  card.dataset.roundIndex = String(roundIndex);
  card.dataset.position = String(position);
  card.style.position = 'absolute';
  card.style.width = `${COLUMN_WIDTH}px`;
  card.style.top = `${topPx}px`;
  card.style.left = '0';

  card.innerHTML = `
    <div class="space-y-1">
      ${playerRowHtml(playerA, topWinner)}
      <div class="text-center text-[11px] font-mono text-gray-500 min-h-[1rem]">${escapeHtml(scoreText || '')}</div>
      ${playerRowHtml(playerB, bottomWinner)}
    </div>
  `;

  return card;
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

function drawConnectors(content, svg) {
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

  const rounds = Array.from(byRound.keys()).sort((a, b) => a - b);
  for (const roundIndex of rounds) {
    const sourceMap = byRound.get(roundIndex);
    const targetMap = byRound.get(roundIndex + 1);
    if (!sourceMap || !targetMap) continue;

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
  }
}

function cleanupContainer(container) {
  const cleanup = cleanupMap.get(container);
  if (!cleanup) return;
  cleanup();
  cleanupMap.delete(container);
}

export function renderBracket(container, payload) {
  if (!container) return;
  cleanupContainer(container);

  const rounds = Array.isArray(payload?.rounds) ? payload.rounds : [];
  if (!payload?.has_draw || !rounds.length) {
    container.innerHTML = '<p class="text-sm text-gray-400 py-6">Draw is not available for this edition.</p>';
    return;
  }

  const firstRoundMap = getRoundMatchMap(rounds[0]);
  const firstFallback = Math.max(...Array.from(firstRoundMap.keys()), rounds[0]?.matches?.length || 0);
  const firstRoundCount = expectedMatchesForRound(rounds[0]?.round, firstFallback);
  const bracketHeight = Math.max(
    BASE_MATCH_HEIGHT,
    firstRoundCount * BASE_MATCH_HEIGHT + Math.max(firstRoundCount - 1, 0) * BASE_GAP
  );

  const content = document.createElement('div');
  content.className = 'teelo-bracket-content relative inline-flex items-start gap-8 min-w-max pb-4';

  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.classList.add('teelo-bracket-connectors');
  svg.style.position = 'absolute';
  svg.style.inset = '0';
  svg.style.pointerEvents = 'none';
  svg.style.zIndex = '0';
  content.appendChild(svg);

  rounds.forEach((round, roundIndex) => {
    const matchMap = getRoundMatchMap(round);
    const fallbackCount = Math.max(...Array.from(matchMap.keys()), round?.matches?.length || 0);
    const expectedCount = expectedMatchesForRound(round?.round, fallbackCount);
    const growth = Math.pow(2, roundIndex);
    const verticalStride = growth * (BASE_MATCH_HEIGHT + BASE_GAP);
    const topOffset = ((growth - 1) * (BASE_MATCH_HEIGHT + BASE_GAP)) / 2;

    const column = document.createElement('div');
    column.className = 'teelo-bracket-round relative z-[1]';
    column.style.width = `${COLUMN_WIDTH}px`;

    const header = document.createElement('div');
    header.className = 'text-xs uppercase tracking-wider text-gray-400 font-bold mb-2 text-center';
    header.style.height = `${HEADER_HEIGHT - 8}px`;
    header.textContent = round?.label || round?.round || '';
    column.appendChild(header);

    const lane = document.createElement('div');
    lane.className = 'relative';
    lane.style.height = `${bracketHeight}px`;
    column.appendChild(lane);

    for (let position = 1; position <= expectedCount; position += 1) {
      const match = matchMap.get(position);
      const topPx = topOffset + (position - 1) * verticalStride;
      lane.appendChild(buildMatchCard(match, roundIndex, position, topPx, !match));
    }

    content.appendChild(column);
  });

  container.innerHTML = '';
  container.appendChild(content);

  const redraw = () => drawConnectors(content, svg);
  const debouncedRedraw = createDebounced(redraw, 100);

  window.requestAnimationFrame(redraw);
  window.addEventListener('resize', debouncedRedraw);

  cleanupMap.set(container, () => {
    window.removeEventListener('resize', debouncedRedraw);
  });
}
