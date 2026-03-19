export function escapeHtml(value) {
  if (value == null) return '';
  const div = document.createElement('div');
  div.textContent = String(value);
  return div.innerHTML;
}

export function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

export function formatShortDate(value) {
  if (!value) return '';
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function parseDateValue(value) {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function formatLocalDate(isoString) {
  const d = parseDateValue(isoString);
  if (!d) return '';
  return d.toLocaleDateString(undefined, { day: '2-digit', month: 'short', year: 'numeric' });
}

export function formatLocalTime(isoString) {
  const d = parseDateValue(isoString);
  if (!d) return '';
  return d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
}

export function slugifyName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\u00C0-\u024F]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}
