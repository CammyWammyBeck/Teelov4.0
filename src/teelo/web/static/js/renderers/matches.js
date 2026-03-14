import { escapeHtml, formatNumber } from '../lib/format.js';

function playerHref(player) {
  if (!player?.id) return null;
  return player.player_url || `/players/${encodeURIComponent(String(player.id))}`;
}

export function buildFallbackTableRows(matches) {
  return matches.map((m) => `
<tr class="hover:bg-surface-hover transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime">
  <td class="px-5 py-3"><div class="min-w-0">${m.tournament_url ? `<a href="${escapeHtml(m.tournament_url)}" class="text-sm font-semibold text-teelo-dark truncate block hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.tournament_name || 'Unknown')}</a>` : `<span class="text-sm font-semibold text-teelo-dark truncate block">${escapeHtml(m.tournament_name || 'Unknown')}</span>`}<span class="text-xs text-content-faint">${escapeHtml(m.round || '')}</span></div></td>
  <td class="px-5 py-3 text-right">${playerHref(m.player_a) ? `<a href="${escapeHtml(playerHref(m.player_a))}" class="text-sm text-teelo-dark hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.player_a?.name || 'Unknown')}</a>` : `<span class="text-sm text-teelo-dark">${escapeHtml(m.player_a?.name || 'Unknown')}</span>`}</td>
  <td class="px-5 py-3 text-center"><span class="inline-block px-2.5 py-1 bg-surface-alt rounded-md text-xs font-mono whitespace-nowrap text-teelo-dark font-semibold">${escapeHtml(m.score || 'vs')}</span></td>
  <td class="px-5 py-3">${playerHref(m.player_b) ? `<a href="${escapeHtml(playerHref(m.player_b))}" class="text-sm text-teelo-dark hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.player_b?.name || 'Unknown')}</a>` : `<span class="text-sm text-teelo-dark">${escapeHtml(m.player_b?.name || 'Unknown')}</span>`}</td>
  <td class="px-5 py-3 text-right"><span class="text-xs text-content-faint whitespace-nowrap">${escapeHtml(m.match_date_display || '')}</span></td>
</tr>`).join('');
}

export function buildFallbackCards(matches) {
  return matches.map((m) => `
<div class="px-4 py-3 border-b border-line-subtle last:border-b-0">
  <div class="text-[13px] font-semibold text-teelo-dark truncate">${m.tournament_url ? `<a href="${escapeHtml(m.tournament_url)}" class="hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.tournament_name || 'Unknown')}</a>` : `${escapeHtml(m.tournament_name || 'Unknown')}`}</div>
  <div class="text-[11px] text-content-faint mb-2">${escapeHtml(m.round || '')}${m.match_date_display ? ` · ${escapeHtml(m.match_date_display)}` : ''}</div>
  <div class="flex items-center justify-between gap-2">
    <div class="min-w-0 flex-1">
      <div class="text-[13px] text-teelo-dark truncate">${playerHref(m.player_a) ? `<a href="${escapeHtml(playerHref(m.player_a))}" class="hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.player_a?.name || 'Unknown')}</a>` : `<span>${escapeHtml(m.player_a?.name || 'Unknown')}</span>`}</div>
      <div class="text-[13px] text-teelo-dark truncate">${playerHref(m.player_b) ? `<a href="${escapeHtml(playerHref(m.player_b))}" class="hover:underline decoration-teelo-lime decoration-2">${escapeHtml(m.player_b?.name || 'Unknown')}</a>` : `<span>${escapeHtml(m.player_b?.name || 'Unknown')}</span>`}</div>
    </div>
    <span class="text-xs font-mono whitespace-nowrap text-teelo-dark font-semibold">${escapeHtml(m.score || 'vs')}</span>
  </div>
</div>`).join('');
}

export function renderMatchesView(els, data, append) {
  const matches = data.matches || [];
  if (!append && matches.length === 0) {
    els.emptyState.classList.remove('hidden');
    els.tableBody.innerHTML = '';
    els.cardsContainer.innerHTML = '';
    return;
  }

  els.emptyState.classList.add('hidden');
  if (append) {
    els.tableBody.insertAdjacentHTML('beforeend', data.table_rows_html || '');
    els.cardsContainer.insertAdjacentHTML('beforeend', data.cards_html || '');
  } else {
    els.tableBody.innerHTML = data.table_rows_html || '';
    els.cardsContainer.innerHTML = data.cards_html || '';
  }
  els.resultsCount.textContent = `${formatNumber(data.total)} match${data.total === 1 ? '' : 'es'}`;
  window.lucide?.createIcons?.();
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
