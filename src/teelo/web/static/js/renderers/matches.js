import { escapeHtml, formatNumber } from '../lib/format.js';

function playerHref(player) {
  if (!player?.id) return null;
  return player.player_url || `/players/${encodeURIComponent(String(player.id))}`;
}

export function buildFallbackTableRows(matches) {
    return matches.map(m => {
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;
        const predHtml = predA != null
            ? `<div class="flex flex-col items-center gap-0.5 text-[11px] leading-tight">
                 <span class="${predA >= predB ? 'text-teelo-dark font-semibold' : 'text-content-faint'}">${predA}%</span>
                 <span class="${predB > predA ? 'text-teelo-dark font-semibold' : 'text-content-faint'}">${predB}%</span>
               </div>`
            : '<span class="text-content-faintest text-[11px]">\u2014</span>';
        const matchUrl = m.match_url || `/matches/${m.id}`;
        return `<tr class="hover:bg-surface-hover/50 transition-colors group border-l-4 border-transparent hover:border-teelo-lime cursor-pointer" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
            <td class="px-5 py-3"><span class="text-sm font-semibold">${escapeHtml(m.tournament_name || '')}</span></td>
            <td class="px-5 py-3 text-right"><span class="text-sm">${escapeHtml(m.player_a?.name || '')}</span></td>
            <td class="px-5 py-3 text-center"><span class="text-xs font-mono">${escapeHtml(m.score || 'vs')}</span></td>
            <td class="px-5 py-3"><span class="text-sm">${escapeHtml(m.player_b?.name || '')}</span></td>
            <td class="px-3 py-3 text-center">${predHtml}</td>
            <td class="px-5 py-3 text-right"><span class="text-xs text-content-faint">${escapeHtml(m.match_date_display || '')}</span></td>
            <td class="pr-3 py-3 w-8"><a href="${escapeHtml(matchUrl)}" class="text-content-faintest group-hover:text-teelo-lime" onclick="event.stopPropagation()"><i data-lucide="chevron-right" class="w-4 h-4"></i></a></td>
        </tr>`;
    }).join('');
}

export function buildFallbackCards(matches) {
    return matches.map(m => {
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;
        const predHtml = predA != null
            ? `<div class="flex items-center gap-2 text-[11px] mb-2">
                 <span class="${predA >= predB ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predA}%</span>
                 <div class="flex-1 h-1 rounded-full bg-surface-muted overflow-hidden"><div class="h-full bg-teelo-lime rounded-full" style="width:${predA}%"></div></div>
                 <span class="${predB > predA ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predB}%</span>
               </div>`
            : '';
        const matchUrl = m.match_url || `/matches/${m.id}`;
        return `<div class="px-4 py-3 border-b border-line-subtle last:border-b-0 cursor-pointer hover:bg-surface-hover/50" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
            <div class="flex items-center gap-2 mb-0.5">
                <span class="text-[13px] font-semibold text-teelo-dark truncate">${escapeHtml(m.tournament_name || '')}</span>
                <a href="${escapeHtml(matchUrl)}" class="ml-auto text-content-faintest" onclick="event.stopPropagation()"><i data-lucide="chevron-right" class="w-4 h-4"></i></a>
            </div>
            ${predHtml}
            <div class="flex items-center">
                <div class="flex-1 min-w-0 space-y-1">
                    <div class="text-[13px]">${escapeHtml(m.player_a?.name || '')}</div>
                    <div class="text-[13px]">${escapeHtml(m.player_b?.name || '')}</div>
                </div>
                <span class="text-xs font-mono ml-3">${escapeHtml(m.score || 'vs')}</span>
            </div>
        </div>`;
    }).join('');
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
