# Match Date/Time Display Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show accurate, timezone-aware match times on all match rows, with consistent sorting (scheduled before unscheduled) across all pages.

**Architecture:** Add `timezone` (IANA string) column to Tournament model, backfill via `geopy` + `timezonefinder`, then expose timezone-aware ISO strings from `serialize_match()` so the browser can convert to the user's local time. Unify sorting logic across all match endpoints to put scheduled matches before unscheduled ones.

**Tech Stack:** Python `zoneinfo` (stdlib), `geopy` + `timezonefinder` (new deps for backfill), PostgreSQL, JavaScript `Intl.DateTimeFormat`.

---

## Current State (Bugs)

1. **Time never shown** — `serialize_match()` formats date-only (`%d %b %Y`), ignoring `scheduled_datetime` which has real times (2,342 matches).
2. **Unscheduled matches sort before scheduled** — `COALESCE(scheduled_date, match_date)` puts NULL-scheduled matches with old `match_date` values first.
3. **Sorting inconsistent across pages** — Home uses `scheduled_datetime` tiebreak; tournament page uses round; matches/player pages ignore `scheduled_date` entirely.
4. **Date filter excludes upcoming** — Filters only on `match_date`, missing matches that only have `scheduled_date`.
5. **No timezone info** — `scheduled_datetime` is naive (tournament-local time), no timezone stored anywhere.

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/teelo/db/models.py` | Modify | Add `timezone` column to Tournament |
| `alembic/versions/20260319_000000_add_tournament_timezone.py` | Create | Migration for timezone column |
| `src/teelo/utils/geo.py` | Modify | Add `city_country_to_timezone()` function |
| `scripts/backfill_tournament_timezones.py` | Create | One-time backfill script |
| `src/teelo/scrape/pipeline.py` | Modify | Wire timezone into tournament create/update |
| `src/teelo/web/services/match_service.py` | Modify | Add time + timezone to serialized output |
| `src/teelo/web/templates/partials/match_rows.html` | Modify | Add `<time>` element for JS hydration |
| `src/teelo/web/static/js/lib/time.js` | Create | Client-side local time conversion |
| `src/teelo/web/static/js/home.js` | Modify | Call time hydration after render |
| `src/teelo/web/static/js/renderers/matches.js` | Modify | Call time hydration after render |
| `src/teelo/web/static/js/controllers/matches_controller.js` | Modify | Call time hydration after render |
| `src/teelo/web/static/js/controllers/player_detail_controller.js` | Modify | Call time hydration after render |
| `src/teelo/web/static/js/controllers/tournament_detail_controller.js` | Modify | Call time hydration after render |
| `src/teelo/web/routers/public.py` | Modify | Fix upcoming sort order |
| `src/teelo/web/routers/matches.py` | Modify | Unify sort, fix date filter |
| `src/teelo/web/routers/tournaments.py` | Modify | Unify upcoming sort |
| `src/teelo/web/services/legacy_main_handlers.py` | Modify | Unify sort for player pages |
| `pyproject.toml` | Modify | Add `geopy`, `timezonefinder` as optional deps |
| `tests/unit/test_match_datetime.py` | Create | Tests for serialization + timezone logic |

---

## Chunk 1: Tournament Timezone Infrastructure

### Task 1: Add `timezone` column to Tournament model

**Files:**
- Modify: `src/teelo/db/models.py:443-445` (Tournament location fields)
- Create: `alembic/versions/20260319_000000_add_tournament_timezone.py`

- [ ] **Step 1: Add column to model**

In `src/teelo/db/models.py`, add after `country_ioc`:

```python
# IANA timezone identifier (e.g., "America/New_York")
# Populated from city/country via geo lookup
timezone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
```

- [ ] **Step 2: Generate Alembic migration**

Run: `alembic revision --autogenerate -m "add tournament timezone column"`

Verify the generated migration adds a single nullable VARCHAR(50) column.

- [ ] **Step 3: Apply migration**

Run: `alembic upgrade head`

- [ ] **Step 4: Commit**

```bash
git add src/teelo/db/models.py alembic/versions/*timezone*
git commit -m "feat: add timezone column to Tournament model"
```

### Task 2: Add timezone lookup utility

**Files:**
- Modify: `src/teelo/utils/geo.py` (add function at end)
- Modify: `pyproject.toml` (add optional deps)
- Create: `tests/unit/test_match_datetime.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_match_datetime.py`:

```python
from teelo.utils.geo import city_country_to_timezone


def test_city_country_to_timezone_known_city():
    assert city_country_to_timezone("Miami", "United States") == "America/New_York"


def test_city_country_to_timezone_european():
    assert city_country_to_timezone("Paris", "France") == "Europe/Paris"


def test_city_country_to_timezone_australian():
    assert city_country_to_timezone("Melbourne", "Australia") == "Australia/Melbourne"


def test_city_country_to_timezone_none_inputs():
    assert city_country_to_timezone(None, None) is None
    assert city_country_to_timezone("", "") is None


def test_city_country_to_timezone_unknown_city():
    # Should return None rather than crash
    assert city_country_to_timezone("Nowhereville", "Fantasyland") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_match_datetime.py -v`
Expected: FAIL — `ImportError` or `AttributeError` (function doesn't exist yet)

- [ ] **Step 3: Add dependencies**

In `pyproject.toml`, add to `[project.optional-dependencies]`:
```toml
geo = ["geopy>=2.4", "timezonefinder>=8.0"]
```

Run: `pip install -e ".[geo]"`

- [ ] **Step 4: Implement `city_country_to_timezone`**

Add to end of `src/teelo/utils/geo.py`:

```python
def city_country_to_timezone(city: str | None, country: str | None) -> str | None:
    """
    Look up IANA timezone for a city/country pair using geocoding.

    Uses geopy Nominatim to geocode city+country, then timezonefinder
    to get the timezone from coordinates. Returns None if lookup fails.

    Results are cached in-process to avoid repeated geocoding calls.
    """
    if not city or not country:
        return None

    cache_key = (city.strip().lower(), country.strip().lower())

    if not hasattr(city_country_to_timezone, "_cache"):
        city_country_to_timezone._cache = {}

    cached = city_country_to_timezone._cache.get(cache_key)
    if cached is not None:
        return cached if cached != "" else None

    try:
        from geopy.geocoders import Nominatim
        from timezonefinder import TimezoneFinder

        if not hasattr(city_country_to_timezone, "_geocoder"):
            city_country_to_timezone._geocoder = Nominatim(
                user_agent="teelo-timezone-lookup", timeout=10,
            )
            city_country_to_timezone._tf = TimezoneFinder()

        location = city_country_to_timezone._geocoder.geocode(
            f"{city}, {country}", exactly_one=True,
        )
        if location is None:
            city_country_to_timezone._cache[cache_key] = ""
            return None

        tz = city_country_to_timezone._tf.timezone_at(
            lat=location.latitude, lng=location.longitude,
        )
        city_country_to_timezone._cache[cache_key] = tz or ""
        return tz
    except Exception:
        city_country_to_timezone._cache[cache_key] = ""
        return None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/unit/test_match_datetime.py -v`
Expected: All 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/teelo/utils/geo.py pyproject.toml tests/unit/test_match_datetime.py
git commit -m "feat: add city_country_to_timezone utility with geocoding"
```

### Task 3: Backfill existing tournaments

**Files:**
- Create: `scripts/backfill_tournament_timezones.py`

- [ ] **Step 1: Write backfill script**

```python
"""
One-time backfill: populate Tournament.timezone from city/country.

Usage:
    python scripts/backfill_tournament_timezones.py [--dry-run]
"""
import argparse
import logging
import sys

from sqlalchemy import select

from teelo.db.models import Tournament
from teelo.db.session import get_session
from teelo.utils.geo import city_country_to_timezone

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with get_session() as session:
        tournaments = session.execute(
            select(Tournament).where(
                Tournament.city.isnot(None),
                Tournament.timezone.is_(None),
            )
        ).scalars().all()

        logger.info("Found %d tournaments to backfill", len(tournaments))
        updated = 0
        failed = 0

        for t in tournaments:
            tz = city_country_to_timezone(t.city, t.country)
            if tz:
                t.timezone = tz
                updated += 1
                logger.info("  %-30s %-20s -> %s", t.city, t.country or "(no country)", tz)
            else:
                failed += 1
                logger.warning("  %-30s %-20s -> FAILED", t.city, t.country or "(no country)")

        logger.info("\nUpdated: %d, Failed: %d", updated, failed)

        if args.dry_run:
            logger.info("Dry run — rolling back")
            session.rollback()
        else:
            session.commit()
            logger.info("Committed")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run with --dry-run to verify**

Run: `python scripts/backfill_tournament_timezones.py --dry-run`
Expected: Shows timezone assignments, no DB changes.

- [ ] **Step 3: Run for real**

Run: `python scripts/backfill_tournament_timezones.py`
Expected: Tournaments updated with timezone strings.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_tournament_timezones.py
git commit -m "feat: add backfill script for tournament timezones"
```

### Task 4: Wire timezone into scraper pipeline

**Files:**
- Modify: `src/teelo/scrape/pipeline.py:168-181` (tournament creation)
- Modify: `src/teelo/scrape/pipeline.py:899-919` (tournament metadata update)

- [ ] **Step 1: Add timezone on tournament creation**

In `get_or_create_edition`, after `city=...` on the new Tournament (line ~178), add:

```python
# Inside the Tournament() constructor call, after the city= line:
timezone=None,  # populated by update_tournament_metadata once city/country are set
```

This is just documentation — timezone needs both city AND country, which aren't always available at creation time.

- [ ] **Step 2: Add timezone population in `update_tournament_metadata`**

After the `country_to_ioc` block (line ~919), add:

```python
    # Populate timezone from city/country if not already set.
    # This uses geocoding (Nominatim) which is rate-limited, so it only
    # runs for tournaments that don't already have a timezone set.
    # The backfill script handles bulk population; this catches new tournaments.
    if tournament.city and tournament.country and not tournament.timezone:
        try:
            from teelo.utils.geo import city_country_to_timezone
            tz = city_country_to_timezone(tournament.city, tournament.country)
            if tz:
                tournament.timezone = tz
        except Exception:
            pass  # timezone is non-critical, don't fail the pipeline
```

Note: The in-process cache on `city_country_to_timezone` prevents repeated Nominatim calls for the same city within a single scraping run. New tournaments are rare (a few per week), so rate limiting is not a practical concern.

- [ ] **Step 3: Commit**

```bash
git add src/teelo/scrape/pipeline.py
git commit -m "feat: populate tournament timezone during scraping"
```

---

## Chunk 2: Serialize Match Times with Timezone

### Task 5: Update `serialize_match()` to include time and timezone

**Files:**
- Modify: `src/teelo/web/services/match_service.py:60-126`
- Modify: `tests/unit/test_match_datetime.py`

The strategy:
- Keep `match_date_display` as the **date-only** string (for completed matches, fallback)
- Add `scheduled_time_utc` — an ISO 8601 string with `Z` suffix when timezone info is available
- Add `scheduled_time_local` — the original tournament-local time string (e.g., "18:00")
- Add `scheduled_tz` — the IANA timezone (e.g., "America/New_York") for the frontend
- The JS will use `scheduled_time_utc` to convert to user's local time; falls back to `match_date_display` when no time is available

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_match_datetime.py`:

```python
from datetime import date, datetime
from unittest.mock import MagicMock

from teelo.web.services.match_service import serialize_match


def _make_mock_match(
    *,
    match_id=1,
    match_date=None,
    scheduled_date=None,
    scheduled_datetime=None,
    tournament_timezone=None,
    status="scheduled",
):
    """Create a minimal mock Match object for serialize_match testing."""
    match = MagicMock()
    match.id = match_id
    match.match_date = match_date
    match.scheduled_date = scheduled_date
    match.scheduled_datetime = scheduled_datetime
    match.status = status
    match.score = None
    match.winner_id = None
    match.round = "R32"
    match.player_a_seed = None
    match.player_b_seed = None
    match.prediction_a = None
    match.temporal_order = 1

    # ELO fields
    match.elo_pre_player_a = None
    match.elo_pre_player_b = None
    match.elo_post_player_a = None
    match.elo_post_player_b = None

    # Player mocks
    pa = MagicMock()
    pa.id = 100
    pa.canonical_name = "Player A"
    pb = MagicMock()
    pb.id = 200
    pb.canonical_name = "Player B"
    match.player_a = pa
    match.player_a_id = 100
    match.player_b = pb
    match.player_b_id = 200

    # Tournament mocks
    tournament = MagicMock()
    tournament.tour = "ATP"
    tournament.gender = "men"
    tournament.name = "Test Open"
    tournament.tournament_code = "test"
    tournament.level = "ATP 250"
    tournament.surface = "Hard"
    tournament.timezone = tournament_timezone

    edition = MagicMock()
    edition.tournament = tournament
    edition.year = 2026
    edition.surface = None

    match.tournament_edition = edition

    return match


def test_serialize_match_with_timezone_produces_utc():
    """When scheduled_datetime and timezone exist, output scheduled_start_utc."""
    match = _make_mock_match(
        scheduled_date=date(2026, 3, 19),
        scheduled_datetime=datetime(2026, 3, 19, 10, 0),  # 10 AM local
        tournament_timezone="America/New_York",
    )
    result = serialize_match(match)
    # 10 AM EDT (UTC-4 in March) = 14:00 UTC
    assert result["scheduled_start_utc"] == "2026-03-19T14:00:00Z"
    assert result["match_date_display"] == "19 Mar 2026"


def test_serialize_match_without_timezone_no_utc():
    """When no timezone, scheduled_start_utc should be None."""
    match = _make_mock_match(
        scheduled_date=date(2026, 3, 19),
        scheduled_datetime=datetime(2026, 3, 19, 10, 0),
        tournament_timezone=None,
    )
    result = serialize_match(match)
    assert result["scheduled_start_utc"] is None


def test_serialize_match_completed_no_time():
    """Completed matches just show date, no time fields."""
    match = _make_mock_match(
        match_date=date(2026, 3, 18),
        status="completed",
    )
    result = serialize_match(match)
    assert result["match_date_display"] == "18 Mar 2026"
    assert result["scheduled_start_utc"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_match_datetime.py::test_serialize_match_with_timezone_produces_utc -v`
Expected: FAIL — `scheduled_start_utc` key not in result

- [ ] **Step 3: Implement timezone-aware serialization**

In `src/teelo/web/services/match_service.py`, add imports at top:

```python
from zoneinfo import ZoneInfo
```

In `serialize_match()`, after line 66 (`display_date = ...`), add the UTC conversion logic:

```python
    # Compute timezone-aware UTC time for scheduled matches
    scheduled_start_utc = None
    if match.scheduled_datetime and te and tournament:
        tz_name = tournament.timezone
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
                # Attach tournament-local timezone to the naive datetime.
                # DST ambiguity is not a practical concern here — tennis matches
                # are scheduled during daytime (10:00-21:00), never during the
                # 1-2 AM DST transition window.
                local_dt = match.scheduled_datetime.replace(tzinfo=tz)
                utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
                scheduled_start_utc = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (KeyError, ValueError):
                pass  # invalid timezone name, skip
```

In the return dict (after `"match_date_display"` line), add:

```python
        "scheduled_start_utc": scheduled_start_utc,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_match_datetime.py -v`
Expected: All tests PASS

- [ ] **Step 5: Update legacy serializer too**

In `src/teelo/web/services/legacy_main_handlers.py`, the `_serialize_match` function (line ~320) has a duplicate serializer. Add the same `scheduled_start_utc` logic:

```python
from zoneinfo import ZoneInfo
```

Same conversion block after `display_date = match.match_date or match.scheduled_date` (line 337), and add `"scheduled_start_utc": scheduled_start_utc` to the return dict.

- [ ] **Step 6: Commit**

```bash
git add src/teelo/web/services/match_service.py src/teelo/web/services/legacy_main_handlers.py tests/unit/test_match_datetime.py
git commit -m "feat: add timezone-aware UTC time to match serialization"
```

---

## Chunk 3: Frontend Time Display

### Task 6: Create client-side time conversion module

**Files:**
- Create: `src/teelo/web/static/js/lib/time.js`

- [ ] **Step 1: Create `time.js` module**

```javascript
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
  // Omit hour12 to use the user's locale default (12h in US/UK, 24h in EU)
};

const DATE_TIME_FORMAT = {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
  hour: 'numeric',
  minute: '2-digit',
  // Omit hour12 to use the user's locale default (12h in US/UK, 24h in EU)
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
```

- [ ] **Step 2: Commit**

```bash
git add src/teelo/web/static/js/lib/time.js
git commit -m "feat: add client-side local time conversion module"
```

### Task 7: Update match row templates to include `<time>` elements

**Files:**
- Modify: `src/teelo/web/templates/partials/match_rows.html:116` (table row date cell)
- Modify: `src/teelo/web/templates/partials/match_rows.html:164` (mobile card date)

- [ ] **Step 1: Update table row date cell**

Replace line 116:
```html
<span class="text-xs text-content-faint whitespace-nowrap">{{ m.match_date_display or '' }}</span>
```

With:
```html
{% if m.scheduled_start_utc %}
<time data-utc="{{ m.scheduled_start_utc }}" class="text-xs text-content-faint whitespace-nowrap">{{ m.match_date_display or '' }}</time>
{% else %}
<span class="text-xs text-content-faint whitespace-nowrap">{{ m.match_date_display or '' }}</span>
{% endif %}
```

- [ ] **Step 2: Update mobile card date**

Replace line 164:
```html
<span>{{ m.match_date_display or '' }}</span>
```

With:
```html
{% if m.scheduled_start_utc %}
<time data-utc="{{ m.scheduled_start_utc }}">{{ m.match_date_display or '' }}</time>
{% else %}
<span>{{ m.match_date_display or '' }}</span>
{% endif %}
```

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/templates/partials/match_rows.html
git commit -m "feat: add time elements with UTC data attributes to match rows"
```

### Task 8: Wire time hydration into all JS entry points

**Files:**
- Modify: `src/teelo/web/static/js/home.js`
- Modify: `src/teelo/web/static/js/renderers/matches.js`
- Modify: JS controllers that render match rows

After every place that inserts match row HTML (via `innerHTML` or `insertAdjacentHTML`), call `hydrateMatchTimes()`.

- [ ] **Step 1: Update `home.js`**

Add import at top:
```javascript
import { hydrateMatchTimes } from './lib/time.js';
```

In `renderSection()`, after `window.lucide?.createIcons?.()` (or at end of function), add:
```javascript
  // Hydrate match times after inserting HTML
  if (tableBodyEl) hydrateMatchTimes(tableBodyEl);
  if (cardsEl) hydrateMatchTimes(cardsEl);
```

Note: Check for the existing `lucide.createIcons()` call as a landmark — add the hydration right after it. If there's no such call in `renderSection`, add it at the end before the function closes.

- [ ] **Step 2: Update `renderers/matches.js`**

In `renderMatchesView()`, after the `window.lucide?.createIcons?.()` call (line ~78), add:

```javascript
import { hydrateMatchTimes } from '../lib/time.js';
```
(at top of file)

And after `window.lucide?.createIcons?.()`:
```javascript
  hydrateMatchTimes(els.tableBody);
  hydrateMatchTimes(els.cardsContainer);
```

- [ ] **Step 3: Update `tournament_detail_controller.js`**

This controller inserts match row HTML directly via `innerHTML` (not through `renderMatchesView`). Add import and hydration:

```javascript
import { hydrateMatchTimes } from '../lib/time.js';
```

After each `innerHTML`/`insertAdjacentHTML` assignment for match rows, and after `window.lucide?.createIcons?.()`, add:

```javascript
hydrateMatchTimes(tableBodyEl);
hydrateMatchTimes(cardsEl);
```

- [ ] **Step 4: Update `player_detail_controller.js`**

Same pattern — this controller also inserts match HTML directly. Add the import and hydration calls after each match row HTML insertion.

- [ ] **Step 5: Update JS fallback renderers**

In `src/teelo/web/static/js/renderers/matches.js`, update `buildFallbackTableRows` and `buildFallbackCards` to emit `<time>` elements when `scheduled_start_utc` is present:

In `buildFallbackTableRows`, replace the date cell:
```javascript
// Old:
<td ...><span class="text-xs text-content-faint">${escapeHtml(m.match_date_display || '')}</span></td>

// New:
<td class="px-5 py-3 text-right">${m.scheduled_start_utc
  ? `<time data-utc="${escapeHtml(m.scheduled_start_utc)}" class="text-xs text-content-faint whitespace-nowrap">${escapeHtml(m.match_date_display || '')}</time>`
  : `<span class="text-xs text-content-faint">${escapeHtml(m.match_date_display || '')}</span>`
}</td>
```

Apply the same pattern to `buildFallbackCards`.

- [ ] **Step 6: Rebuild Tailwind CSS** (in case `<time>` element needs styles)

Run: `npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify`

- [ ] **Step 7: Commit**

```bash
git add src/teelo/web/static/js/
git commit -m "feat: wire time hydration into all match row render paths"
```

---

## Chunk 4: Fix Sorting and Date Filters

### Task 9: Fix upcoming match sort order (scheduled before unscheduled)

**Files:**
- Modify: `src/teelo/web/routers/public.py:207-211` (home upcoming)
- Modify: `src/teelo/web/routers/public.py:648-653` (home API upcoming)
- Modify: `src/teelo/web/routers/tournaments.py:638-642` (tournament upcoming)
- Modify: `src/teelo/web/services/legacy_main_handlers.py` (player page)
- Modify: `src/teelo/web/routers/matches.py:88` (matches page)

The unified upcoming sort should be:
1. `scheduled_date IS NULL` ASC — scheduled matches first (0), unscheduled last (1)
2. `scheduled_date` ASC NULLS LAST — earliest scheduled date first
3. `scheduled_datetime` ASC NULLS LAST — earliest time first
4. `Match.id` ASC — stable tiebreak

The unified completed/historical sort should be:
1. `COALESCE(match_date, scheduled_date)` DESC NULLS LAST
2. `temporal_order` DESC NULLS LAST
3. `Match.id` DESC

- [ ] **Step 1: Define shared sort expressions**

Create a helper in `src/teelo/web/services/match_service.py`:

```python
from sqlalchemy import case


def upcoming_sort_expressions():
    """Standard sort order for upcoming matches: scheduled first, then by date/time."""
    has_schedule = case(
        (Match.scheduled_date.isnot(None), 0),
        else_=1,
    )
    return [
        has_schedule.asc(),
        Match.scheduled_date.asc().nullslast(),
        Match.scheduled_datetime.asc().nullslast(),
        Match.id.asc(),
    ]


def completed_sort_expressions():
    """Standard sort order for completed matches: most recent first."""
    return [
        func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
        Match.temporal_order.desc().nullslast(),
        Match.id.desc(),
    ]
```

Add required imports: `from sqlalchemy import case, func`

- [ ] **Step 2: Update home page upcoming sort**

In `src/teelo/web/routers/public.py`, replace the `.order_by(...)` for upcoming queries (both in `home_api_upcoming` around line 207, and `home_api` around line 648) with:

```python
from teelo.web.services.match_service import upcoming_sort_expressions

.order_by(*upcoming_sort_expressions())
```

- [ ] **Step 3: Update home page completed sort**

Replace completed `.order_by(...)` in both `home_api_completed` and `home_api` with:

```python
from teelo.web.services.match_service import completed_sort_expressions

.order_by(*completed_sort_expressions())
```

- [ ] **Step 4: Update tournament upcoming sort**

In `src/teelo/web/routers/tournaments.py`, at line ~638, replace the upcoming sort with:

```python
from teelo.web.services.match_service import upcoming_sort_expressions, completed_sort_expressions
```

For `status_key == "completed"`: keep the round-based sort (this is tournament-specific and correct):
```python
fetch_q = fetch_q.order_by(
    round_sort.desc(),
    Match.match_date.desc().nullslast(),
    Match.id.desc(),
)
```

For the `else` (upcoming): replace with:
```python
fetch_q = fetch_q.order_by(*upcoming_sort_expressions())
```

- [ ] **Step 5: Update matches page sort**

In `src/teelo/web/routers/matches.py`, line 88. The matches page needs to handle both upcoming and completed in the same query. Replace:

```python
matches = fetch_q.order_by(Match.match_date.desc().nullslast(), Match.temporal_order.desc().nullslast(), Match.id.desc())
```

With logic that checks the status filter:

```python
from teelo.web.services.match_service import upcoming_sort_expressions, completed_sort_expressions

# Determine sort based on requested statuses
upcoming_statuses = set(get_status_group("upcoming"))
if statuses and set(statuses).issubset(upcoming_statuses):
    sort_exprs = upcoming_sort_expressions()
else:
    sort_exprs = completed_sort_expressions()
matches = fetch_q.order_by(*sort_exprs).offset(offset).limit(per_page).all()
```

- [ ] **Step 6: Update player detail sort**

In `src/teelo/web/services/legacy_main_handlers.py`, find the match query order_by (around line 557) and replace with `completed_sort_expressions()`. The player page mostly shows historical matches, so completed sort is correct. If it has an upcoming tab, wire in `upcoming_sort_expressions()` for that path.

- [ ] **Step 7: Commit**

```bash
git add src/teelo/web/services/match_service.py src/teelo/web/routers/public.py src/teelo/web/routers/matches.py src/teelo/web/routers/tournaments.py src/teelo/web/services/legacy_main_handlers.py
git commit -m "fix: unify match sorting - scheduled before unscheduled"
```

### Task 10: Fix date filter to include `scheduled_date`

**Files:**
- Modify: `src/teelo/web/routers/matches.py:58-61`
- Modify: `src/teelo/web/services/legacy_main_handlers.py` (same pattern)

- [ ] **Step 1: Update date filter in matches router**

In `src/teelo/web/routers/matches.py`, replace lines 58-61:

```python
        if resolved_from:
            q = q.filter(Match.match_date >= resolved_from)
        if resolved_to:
            q = q.filter(Match.match_date <= resolved_to)
```

With:

```python
        if resolved_from:
            q = q.filter(
                or_(
                    Match.match_date >= resolved_from,
                    and_(Match.match_date.is_(None), Match.scheduled_date >= resolved_from),
                )
            )
        if resolved_to:
            q = q.filter(
                or_(
                    Match.match_date <= resolved_to,
                    and_(Match.match_date.is_(None), Match.scheduled_date <= resolved_to),
                )
            )
```

Ensure `and_` is imported (it already is in this file).

- [ ] **Step 2: Apply same fix to legacy handlers**

Find the equivalent date filter in `legacy_main_handlers.py` (around line 548-550) and apply the same change.

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/routers/matches.py src/teelo/web/services/legacy_main_handlers.py
git commit -m "fix: date filter includes scheduled_date for upcoming matches"
```

---

## Chunk 5: Verification

### Task 11: End-to-end verification

- [ ] **Step 1: Run full test suite**

Run: `pytest -v`
Expected: All tests pass.

- [ ] **Step 2: Run linting**

Run: `ruff check src/teelo/web/services/match_service.py src/teelo/web/routers/public.py src/teelo/web/routers/matches.py src/teelo/web/routers/tournaments.py`
Expected: No errors.

- [ ] **Step 3: Start dev server and visually verify**

Run: `uvicorn teelo.api.main:app --reload`

Check these pages:
1. **Home page** — Upcoming matches should show scheduled matches first with local times (e.g., "Today, 14:00"). Unscheduled matches appear after.
2. **Matches page** — Date column shows times for scheduled matches. Filtering by date range includes upcoming matches.
3. **Tournament detail** — Upcoming tab shows scheduled matches first, with times.
4. **Player detail** — Match history shows consistent date formatting.

- [ ] **Step 4: Verify with database query**

Run the updated `claude_testing.py` to confirm:
- Upcoming matches with `scheduled_datetime` + tournament timezone produce valid `scheduled_start_utc`
- Sort order puts scheduled before unscheduled

- [ ] **Step 5: Rebuild CSS and commit any remaining changes**

Run: `npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify`

```bash
git add -A
git commit -m "chore: final verification and CSS rebuild"
```
