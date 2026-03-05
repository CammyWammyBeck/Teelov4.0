export const MULTI_VALUE_FILTERS = ['tour', 'surface', 'level', 'round', 'status'];

export function serializeQueryParams(paramsObject) {
  const params = new URLSearchParams();
  Object.entries(paramsObject).forEach(([key, value]) => {
    if (value == null || value === '') return;
    if (Array.isArray(value)) {
      if (!value.length) return;
      params.set(key, value.join(','));
      return;
    }
    params.set(key, String(value));
  });
  return params.toString();
}

export function parseMultiFilterParam(value) {
  if (!value) return [];
  return value.split(',').map((v) => v.trim()).filter(Boolean);
}

export function toggleValue(list, value) {
  return list.includes(value) ? list.filter((item) => item !== value) : [...list, value];
}

export function removeByKey(state, removeKey) {
  const [name, ...rest] = removeKey.split(':');
  const value = rest.join(':');
  const next = { ...state };

  if (MULTI_VALUE_FILTERS.includes(name)) {
    next[name] = (next[name] || []).filter((item) => item !== value);
  } else if (name === 'gender') {
    next.gender = '';
  } else if (name === 'date_preset') {
    next.date_preset = '';
  } else if (name === 'date_range') {
    next.date_preset = '';
    next.date_from = '';
    next.date_to = '';
  }

  return next;
}
