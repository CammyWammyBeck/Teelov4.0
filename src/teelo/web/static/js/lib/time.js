/**
 * Hydrate <time> elements with data-utc attributes to show local time.
 *
 * Usage: After inserting match rows HTML, call hydrateMatchTimes()
 * to convert UTC timestamps to the user's local time.
 *
 * Elements must have: data-utc="2026-03-19T14:00:00Z"
 * Optional: data-date-fallback="19 Mar 2026" (shown if no UTC time)
 */

const SHORT_TIME_FORMAT = {
  hour: 'numeric',
  minute: '2-digit',
};

const DATE_ONLY_FORMAT = {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
};

/**
 * Format a UTC ISO string to the user's local time.
 * Returns object with { datePart, timePart, full } for flexible display.
 */
export function formatLocalTime(utcIso) {
  if (!utcIso) return null;
  const d = new Date(utcIso);
  if (Number.isNaN(d.getTime())) return null;

  const now = new Date();
  const isToday = d.toDateString() === now.toDateString();
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  const isTomorrow = d.toDateString() === tomorrow.toDateString();

  const timePart = d.toLocaleTimeString(undefined, SHORT_TIME_FORMAT);
  const datePart = d.toLocaleDateString(undefined, DATE_ONLY_FORMAT);

  let dateLabel;
  if (isToday) {
    dateLabel = 'Today';
  } else if (isTomorrow) {
    dateLabel = 'Tomorrow';
  } else {
    dateLabel = datePart;
  }

  return {
    datePart,
    timePart,
    dateLabel,
    full: `${dateLabel}, ${timePart}`,
  };
}

/**
 * Hydrate all <time data-utc="..."> elements within a container.
 * Call after inserting server-rendered match row HTML.
 */
export function hydrateMatchTimes(container) {
  if (!container) return;
  const elements = container.querySelectorAll('time[data-utc]');
  for (const el of elements) {
    const utc = el.dataset.utc;
    const result = formatLocalTime(utc);
    if (result) {
      el.textContent = result.full;
      el.setAttribute('datetime', utc);
      el.title = `${result.datePart}, ${result.timePart} (your local time)`;
    }
    // If no result, the server-rendered fallback text remains
  }
}
