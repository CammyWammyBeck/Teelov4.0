// Delegated click/keyboard navigation for match rows.
// Any element carrying [data-match-url] becomes clickable as a whole; nested
// anchors or betting-odds inputs continue to behave normally via their own
// stopPropagation handlers or the guards below.

function findRow(target) {
  return target.closest?.('[data-match-url]') ?? null;
}

function navigate(row) {
  const url = row?.dataset?.matchUrl;
  if (url) window.location.href = url;
}

function handleClick(event) {
  if (event.defaultPrevented) return;
  if (event.button !== undefined && event.button !== 0) return;
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  const target = event.target;
  if (!target) return;
  if (target.closest('a, button, input, select, textarea, label')) return;
  const row = findRow(target);
  if (row) navigate(row);
}

function handleKeydown(event) {
  if (event.key !== 'Enter' && event.key !== ' ') return;
  const target = event.target;
  if (!target) return;
  if (target.closest('a, button, input, select, textarea')) return;
  const row = findRow(target);
  if (!row) return;
  event.preventDefault();
  navigate(row);
}

export function enableMatchRowNavigation(scope = document) {
  if (!scope || scope.__teeloMatchRowNavBound) return;
  scope.__teeloMatchRowNavBound = true;
  scope.addEventListener('click', handleClick);
  scope.addEventListener('keydown', handleKeydown);
}
