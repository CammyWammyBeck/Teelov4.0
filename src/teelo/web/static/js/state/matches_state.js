import { MULTI_VALUE_FILTERS, parseMultiFilterParam, serializeQueryParams } from '../lib/filters.js';

export function createMatchesState() {
  return {
    gender: '',
    tour: [],
    surface: [],
    level: [],
    round: [],
    status: [],
    player_id: null,
    player_name: '',
    player_a_id: null,
    player_b_id: null,
    tournament: '',
    date_from: '',
    date_to: '',
    date_preset: '',
    page: 1,
    per_page: 50,
    has_more: true,
    loading: false,
  };
}

export function hydrateFromUrl(state, search) {
  const params = new URLSearchParams(search);
  MULTI_VALUE_FILTERS.forEach((k) => { state[k] = parseMultiFilterParam(params.get(k)); });
  state.gender = params.get('gender') || '';
  state.player_id = params.get('player_id') ? Number(params.get('player_id')) : null;
  state.player_name = params.get('player_name') || '';
  state.player_a_id = params.get('player_a_id') ? Number(params.get('player_a_id')) : null;
  state.player_b_id = params.get('player_b_id') ? Number(params.get('player_b_id')) : null;
  state.tournament = params.get('tournament') || '';
  state.date_from = params.get('date_from') || '';
  state.date_to = params.get('date_to') || '';
  state.date_preset = params.get('date_preset') || '';
}

export function toApiQuery(state) {
  return serializeQueryParams({
    gender: state.gender,
    tour: state.tour,
    surface: state.surface,
    level: state.level,
    round: state.round,
    status: state.status,
    player_id: state.player_id,
    player: state.player_id ? '' : state.player_name,
    player_a_id: state.player_a_id,
    player_b_id: state.player_b_id,
    tournament: state.tournament,
    date_from: state.date_from,
    date_to: state.date_to,
    date_preset: state.date_preset,
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
    round: state.round,
    status: state.status,
    player_id: state.player_id,
    player_name: state.player_name,
    player_a_id: state.player_a_id,
    player_b_id: state.player_b_id,
    tournament: state.tournament,
    date_from: state.date_from,
    date_to: state.date_to,
    date_preset: state.date_preset,
  });
}
