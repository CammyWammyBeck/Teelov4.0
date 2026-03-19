/**
 * Hydrate elements with data-utc-date / data-utc-time attributes to show local time.
 *
 * Usage: After inserting match rows HTML, call hydrateMatchTimes()
 * to convert UTC timestamps to the user's local time.
 *
 * Elements must have:
 *   data-utc-date="2026-03-19T14:00:00Z"  — shows localized date (Today/Tomorrow/date)
 *   data-utc-time="2026-03-19T14:00:00Z"  — shows localized time
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
 * Returns object with { datePart, timePart, dateLabel, full } for flexible display.
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
 * Hydrate all [data-utc-date] and [data-utc-time] elements within a container.
 * Call after inserting server-rendered match row HTML.
 */
export function hydrateMatchTimes(container) {
  if (!container) return;
  // Hydrate date elements
  for (const el of container.querySelectorAll('[data-utc-date]')) {
    const result = formatLocalTime(el.dataset.utcDate);
    if (result) {
      el.textContent = result.dateLabel;
      el.title = `${result.datePart} (your local date)`;
    }
  }
  // Hydrate time elements
  for (const el of container.querySelectorAll('[data-utc-time]')) {
    const result = formatLocalTime(el.dataset.utcTime);
    if (result) {
      el.textContent = result.timePart;
      el.title = `${result.datePart}, ${result.timePart} (your local time)`;
    }
  }
}
