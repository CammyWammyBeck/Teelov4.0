import { escapeHtml, formatNumber } from '../lib/format.js';
import { hydrateMatchTimes } from '../lib/time.js';

export function renderMatchesView(els, data, append) {
  // Hide skeleton and error state on any render
  document.getElementById('skeleton-loading')?.classList.add('hidden');
  document.getElementById('error-state')?.classList.add('hidden');

  const matches = data.matches || [];
  if (!append && matches.length === 0) {
    els.emptyState.classList.remove('hidden');
    els.tableBody.innerHTML = '';
    els.cardsContainer.innerHTML = '';
    if (els.scrollSentinel) els.scrollSentinel.classList.add('hidden');
    els.resultsCount.textContent = '0 matches';
    return;
  }

  els.emptyState.classList.add('hidden');

  const tableHtml = data.table_rows_html || '';
  const cardsHtml = data.cards_html || '';

  if (append) {
    els.tableBody.insertAdjacentHTML('beforeend', tableHtml);
    els.cardsContainer.insertAdjacentHTML('beforeend', cardsHtml);
  } else {
    els.tableBody.innerHTML = tableHtml;
    els.cardsContainer.innerHTML = cardsHtml;
  }

  if (data.total != null) {
    els.resultsCount.textContent = `${formatNumber(data.total)} match${data.total === 1 ? '' : 'es'}`;
  } else {
    const currentCount = els.tableBody.querySelectorAll('tr').length;
    els.resultsCount.textContent = data.has_more ? `${formatNumber(currentCount)}+ matches` : `${formatNumber(currentCount)} match${currentCount === 1 ? '' : 'es'}`;
  }

  if (els.scrollSentinel) {
    els.scrollSentinel.classList.toggle('hidden', !data.has_more);
  }

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
