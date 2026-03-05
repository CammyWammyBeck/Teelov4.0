import { escapeHtml, formatNumber } from '../lib/format.js';

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
  if (typeof lucide !== 'undefined') lucide.createIcons();
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
