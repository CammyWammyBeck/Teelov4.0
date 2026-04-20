// Delegated mouse-click navigation for match rows.
// The row is a convenience click-target for mouse users. Keyboard users
// reach the match page via the existing nested links (player names,
// tournament name, chevron), which is why rows themselves are not focusable
// and no keydown handler is registered.

function handleClick(event) {
  if (event.defaultPrevented) return;
  if (event.button !== undefined && event.button !== 0) return;
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  const target = event.target;
  if (!target?.closest) return;
  if (target.closest('a, button, input, select, textarea, label')) return;
  const row = target.closest('[data-match-url]');
  const url = row?.dataset?.matchUrl;
  if (url) window.location.href = url;
}

export function enableMatchRowNavigation(scope = document) {
  if (!scope || scope.__teeloMatchRowNavBound) return;
  scope.__teeloMatchRowNavBound = true;
  scope.addEventListener('click', handleClick);
}
