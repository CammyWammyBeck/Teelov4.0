export async function getJson(url, options = {}) {
  const response = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json', ...(options.headers || {}) },
    ...options,
  });

  let payload;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    const message = payload?.detail || payload?.message || `Request failed: ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }

  return payload;
}
