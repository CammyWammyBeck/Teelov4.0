import test from 'node:test';
import assert from 'node:assert/strict';
import { parseMultiFilterParam, removeByKey, serializeQueryParams } from '../lib/filters.js';

global.document = {
  createElement() {
    return {
      set textContent(v) { this._v = String(v); },
      get innerHTML() { return this._v.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;'); },
    };
  },
};

const { escapeHtml } = await import('../lib/format.js');

test('escapeHtml escapes dangerous characters', () => {
  assert.equal(escapeHtml('<script>"x"</script>'), '&lt;script&gt;&quot;x&quot;&lt;/script&gt;');
});

test('serializeQueryParams serializes arrays and scalars', () => {
  assert.equal(serializeQueryParams({ surface: ['hard', 'clay'], page: 2, empty: '' }), 'surface=hard%2Cclay&page=2');
});

test('parseMultiFilterParam + removeByKey transform filter state', () => {
  assert.deepEqual(parseMultiFilterParam('ATP, WTA ,, ITF'), ['ATP', 'WTA', 'ITF']);
  const next = removeByKey({ tour: ['ATP', 'ITF'], gender: 'men', date_from: '2024-01-01', date_to: '2024-02-01' }, 'tour:ATP');
  assert.deepEqual(next.tour, ['ITF']);
});
