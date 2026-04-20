export function byId(id) {
  if (!id) return null;
  return document.getElementById(id);
}

export function query(selector, root = document) {
  return root.querySelector(selector);
}

export function queryAll(selector, root = document) {
  return Array.from(root.querySelectorAll(selector));
}

export function toggleHidden(el, hidden) {
  if (!el) return;
  el.classList.toggle('hidden', !!hidden);
}

export function setSectionLoading(spinnerId, contentIds, isLoading) {
  toggleHidden(byId(spinnerId), !isLoading);
  contentIds.forEach((id) => toggleHidden(byId(id), isLoading));
}
