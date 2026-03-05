import { byId, queryAll } from '../lib/dom.js';
import { getJson } from '../lib/http.js';
import { escapeHtml } from '../lib/format.js';

function createState() { return { page: 0, loading: false, hasMore: true }; }

export function initRankingsPage() {
  const state = { men: createState(), women: createState(), activeMobileTab: 'men', includeInactive: false, surface: 'overall' };
  const els = {
    menBody: byId('men-body'), womenBody: byId('women-body'),
    mobileMenBody: byId('mobile-men-body'), mobileWomenBody: byId('mobile-women-body'),
    menSentinel: byId('men-sentinel'), womenSentinel: byId('women-sentinel'),
    mobileMenSentinel: byId('mobile-men-sentinel'), mobileWomenSentinel: byId('mobile-women-sentinel'),
    tabs: queryAll('.rankings-tab'), inactiveToggle: byId('include-inactive-toggle'),
    surfaceButtons: queryAll('.rankings-surface-btn'),
  };
  if (!els.menBody) return;

  async function fetchGender(gender) {
    const st = state[gender];
    if (st.loading || !st.hasMore) return;
    st.loading = true; st.page += 1;
    try {
      const data = await getJson(`/api/rankings?gender=${gender}&page=${st.page}&per_page=50&surface=${encodeURIComponent(state.surface)}${state.includeInactive ? '&include_inactive=true' : ''}`);
      st.hasMore = !!data.has_more;
      (gender === 'men' ? els.menBody : els.womenBody).insertAdjacentHTML('beforeend', data.table_rows_html || '');
      const mobileHtml = (data.players || []).map((p) => `<tr><td>${p.rank}</td><td><a href="/players/${p.id}">${escapeHtml(p.name)}</a></td><td>${p.rating}</td></tr>`).join('');
      (gender === 'men' ? els.mobileMenBody : els.mobileWomenBody).insertAdjacentHTML('beforeend', mobileHtml);
    } catch (e) { console.error(e); st.hasMore = false; }
    st.loading = false;
  }

  function reset() {
    ['men', 'women'].forEach((g) => { state[g] = createState(); (g === 'men' ? els.menBody : els.womenBody).innerHTML = ''; (g === 'men' ? els.mobileMenBody : els.mobileWomenBody).innerHTML = ''; });
    fetchGender('men'); fetchGender('women');
  }

  els.tabs.forEach((tab) => tab.addEventListener('click', () => { state.activeMobileTab = tab.dataset.gender; }));
  els.inactiveToggle?.addEventListener('change', () => { state.includeInactive = els.inactiveToggle.checked; reset(); });
  els.surfaceButtons.forEach((btn) => btn.addEventListener('click', () => { state.surface = btn.dataset.surface || 'overall'; reset(); }));

  const obs = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting) {
      if (entries[0].target === els.menSentinel || (entries[0].target === els.mobileMenSentinel && state.activeMobileTab === 'men')) fetchGender('men');
      if (entries[0].target === els.womenSentinel || (entries[0].target === els.mobileWomenSentinel && state.activeMobileTab === 'women')) fetchGender('women');
    }
  }, { rootMargin: '260px' });
  [els.menSentinel, els.womenSentinel, els.mobileMenSentinel, els.mobileWomenSentinel].filter(Boolean).forEach((s) => obs.observe(s));

  fetchGender('men');
  fetchGender('women');
}
