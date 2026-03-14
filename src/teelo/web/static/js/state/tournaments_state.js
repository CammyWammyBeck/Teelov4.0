import { parseMultiFilterParam, serializeQueryParams } from '../lib/filters.js';

export const TOURNAMENT_MULTI_VALUE_FILTERS = ['tour', 'surface', 'level', 'status', 'year'];

export function createTournamentsState() {
  return {
    gender: '',
    tour: [],
    surface: [],
    level: [],
    status: [],
    year: [],
    tournament: '',
    page: 1,
    per_page: 24,
    has_more: true,
    loading: false,
  };
}

export function hydrateFromUrl(state, search) {
  const params = new URLSearchParams(search);
  TOURNAMENT_MULTI_VALUE_FILTERS.forEach((key) => {
    state[key] = parseMultiFilterParam(params.get(key));
  });
  state.gender = params.get('gender') || '';
  state.tournament = params.get('tournament') || '';
}

export function toApiQuery(state) {
  return serializeQueryParams({
    gender: state.gender,
    tour: state.tour,
    surface: state.surface,
    level: state.level,
    status: state.status,
    year: state.year,
    tournament: state.tournament,
    page: state.page,
    per_page: state.per_page,
  });
}

export function toUrlQuery(state) {
  return serializeQueryParams({
    gender: state.gender,
    tour: state.tour,
    surface: state.surface,
    level: state.level,
    status: state.status,
    year: state.year,
    tournament: state.tournament,
  });
}
