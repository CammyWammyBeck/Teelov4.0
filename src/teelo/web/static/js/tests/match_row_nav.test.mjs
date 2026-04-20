import test from 'node:test';
import assert from 'node:assert/strict';

// Minimal DOM stubs — the helper only touches window.location.href,
// scope.addEventListener, event.target.closest, and row.dataset.matchUrl.
function makeScope() {
  const handlers = { click: [], keydown: [] };
  return {
    addEventListener(type, fn) {
      if (!handlers[type]) handlers[type] = [];
      handlers[type].push(fn);
    },
    fire(type, event) {
      for (const fn of handlers[type] || []) fn(event);
    },
  };
}

// Target mimics an element's `closest(selector)` contract for the selectors
// the helper actually uses.
function makeTarget({ matchUrl = null, nestedTag = null } = {}) {
  return {
    closest(sel) {
      if (sel === '[data-match-url]') {
        return matchUrl ? { dataset: { matchUrl } } : null;
      }
      if (!nestedTag) return null;
      const types = sel.split(',').map((s) => s.trim());
      return types.includes(nestedTag) ? { tagName: nestedTag.toUpperCase() } : null;
    },
  };
}

let navigations = [];
global.window = {
  get location() {
    return {
      set href(url) {
        navigations.push(url);
      },
    };
  },
};
global.document = { __teeloMatchRowNavBound: false };

const { enableMatchRowNavigation } = await import('../lib/match_row_nav.js');

test('click on row body navigates to match url', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 0,
    target: makeTarget({ matchUrl: '/matches/42' }),
  });
  assert.deepEqual(navigations, ['/matches/42']);
});

test('click on nested anchor does not navigate', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 0,
    target: makeTarget({ matchUrl: '/matches/42', nestedTag: 'a' }),
  });
  assert.deepEqual(navigations, []);
});

test('click on nested input does not navigate', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 0,
    target: makeTarget({ matchUrl: '/matches/42', nestedTag: 'input' }),
  });
  assert.deepEqual(navigations, []);
});

test('cmd/ctrl/shift/alt click does not navigate', () => {
  for (const mod of ['metaKey', 'ctrlKey', 'shiftKey', 'altKey']) {
    navigations = [];
    const scope = makeScope();
    enableMatchRowNavigation(scope);
    scope.fire('click', {
      button: 0,
      [mod]: true,
      target: makeTarget({ matchUrl: '/matches/42' }),
    });
    assert.deepEqual(navigations, [], `modifier ${mod} should skip navigation`);
  }
});

test('middle/right click does not navigate', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 1,
    target: makeTarget({ matchUrl: '/matches/42' }),
  });
  assert.deepEqual(navigations, []);
});

test('click outside any row does not navigate', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 0,
    target: makeTarget({ matchUrl: null }),
  });
  assert.deepEqual(navigations, []);
});

test('double-binding on the same scope is a no-op', () => {
  navigations = [];
  const scope = makeScope();
  enableMatchRowNavigation(scope);
  enableMatchRowNavigation(scope);
  scope.fire('click', {
    button: 0,
    target: makeTarget({ matchUrl: '/matches/42' }),
  });
  assert.deepEqual(navigations, ['/matches/42']);
});
