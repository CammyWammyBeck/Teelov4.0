# Tournament Detail Page Implementation Plan (Teelo v4.0)

Date: 2026-03-08  
Scope: Add a new tournament edition detail experience with matches/draw tabs and edition history using FastAPI + Jinja2 + Tailwind + vanilla JS patterns already used in Teelo.

## Goals
- Add canonical tournament edition URLs:
  - `/tournaments/{tournament_code}` -> redirects to latest edition
  - `/tournaments/{tournament_code}/{year}` -> tournament edition detail page
- Add supporting APIs for matches, draw, and edition history.
- Reuse existing match table/card rendering where possible (`partials/match_rows.html`, existing `table_rows_html` + `cards_html` API pattern).
- Keep page structure and JS architecture consistent with `player_detail` and `matches` pages.

## Existing Patterns to Reuse
- Router registration from `src/teelo/web/main.py` and split router files in `src/teelo/web/routers/`.
- Match API + pagination + status normalization from `src/teelo/web/routers/matches.py` and `src/teelo/match_statuses.py`.
- Template context style from `src/teelo/web/routers/public.py` (passing `request`, `now`, `current_path`).
- Match serialization from `src/teelo/web/services/match_service.py` (`serialize_match`).
- Detail-page JS composition from `src/teelo/web/static/js/player_detail.js` + controller pattern in `controllers/player_detail_controller.js`.
- Reusable Jinja macros in `src/teelo/web/templates/partials/match_rows.html` (`circuit_bg`, `circuit_label`, `render_table_rows`, `render_cards`).
- Existing utility modules:
  - DOM helpers: `src/teelo/web/static/js/lib/dom.js`
  - HTTP helper: `src/teelo/web/static/js/lib/http.js`
  - formatting: `src/teelo/web/static/js/lib/format.js`

## Data + Query Design Decisions
- Tournament identity for routes should use `Tournament.tournament_code`.
- Edition identity should use `(Tournament.tournament_code, TournamentEdition.year)`.
- Surface display should use `coalesce(TournamentEdition.surface, Tournament.surface)` (existing pattern in multiple endpoints).
- Matches tab status mapping:
  - `completed` filter -> `get_status_group("historical_default")`
  - `upcoming` filter -> `get_status_group("upcoming")`
- Draw availability gate:
  - Draw tab is shown only when at least one edition match has non-null `draw_position` and recognized main-draw `round` values.
- Champion/runner-up and final score:
  - Derived from final (`round == "F"`) match in the edition (latest by date/id fallback).

## Task 1. Router + HTML Page Route

### Files to create/modify
- Create: `src/teelo/web/routers/tournaments.py`
- Modify: `src/teelo/web/main.py`

### Implementation approach
- Add a dedicated tournaments router (same style as current routers with `APIRouter`).
- Register router in `main.py` (`app.include_router(tournaments_router)`).
- Add route `GET /tournaments/{tournament_code}`:
  - Query editions for that code ordered by `year DESC`.
  - Redirect (302/307) to `/tournaments/{tournament_code}/{latest_year}`.
  - Return 404 if tournament code not found.
- Add route `GET /tournaments/{tournament_code}/{year}`:
  - Query tournament + edition + edition count + draw-availability bit.
  - Query/derive final summary for header (champion + score).
  - Render `tournament_detail.html` with:
    - `request`, `now`, `current_path`
    - normalized `tournament`/`edition` display payload
    - `years` list for selector
    - flags: `has_draw`, `default_tab`

### Notes
- Follow current HTML route context conventions used in `public.py`.
- Keep error behavior consistent with existing app-level 404 template handling.

## Task 2. API Endpoints

### Files to create/modify
- Modify: `src/teelo/web/routers/tournaments.py`
- Optional create (if logic grows): `src/teelo/web/services/tournament_service.py`

### Endpoint A: `GET /api/tournaments/{code}/{year}/matches?status=completed|upcoming&page=&per_page=`
- Query `Match` joined to `TournamentEdition` + `Tournament` by code/year.
- Normalize status filter:
  - `completed` -> historical statuses (`completed`, `retired`, `walkover`, `default`)
  - `upcoming` -> (`upcoming`, `scheduled`)
  - invalid/missing defaults to `completed` for this tab.
- Sorting:
  - Completed: chronological progression (round + date), grouped by round in response.
  - Upcoming: nearest scheduled first (`coalesce(scheduled_date, match_date)` ascending), still grouped by round.
- Response shape:
  - `matches` (serialized via `serialize_match`)
  - `table_rows_html`, `cards_html` rendered via `partials/match_rows.html`
  - `total`, `page`, `per_page`, `has_more`
  - `groups` metadata (round buckets for tab grouping headers in JS)

### Endpoint B: `GET /api/tournaments/{code}/{year}/draw`
- Query all edition matches where `draw_position IS NOT NULL` and round is in bracket rounds (`R128`, `R64`, `R32`, `R16`, `QF`, `SF`, `F`).
- Include players, seeds, status, score, winner.
- Build a bracket payload grouped by round with sorted positions.
- Include derived linkage fields:
  - `slot_index = draw_position`
  - `next_slot_index = ceil(draw_position / 2)` (for all rounds except `F`)
  - `pair_slot_indices = (2p-1, 2p)` inverse relation used by renderer when needed
- Return precomputed metadata:
  - `round_order` and display labels
  - `max_slots_by_round`
  - `has_draw`
- Fallback behavior:
  - If no draw rows, return `{ has_draw: false, rounds: [] }` with 200.

### Endpoint C: `GET /api/tournaments/{code}/editions`
- Query all editions for tournament code ordered by year DESC.
- Derive per-year final data (champion, runner-up, score, surface) from `round == "F"` match join.
- Include `edition_url` for direct linking.
- Return JSON for table render + year selector sync.

### Query/Performance details
- Use `joinedload`/`contains_eager` patterns already present in routers.
- Use lightweight count query + paginated fetch pattern from `matches.py`.
- Keep payload fields minimal and compatible with current match row macros.

## Task 3. Tournament Detail Template

### Files to create/modify
- Create: `src/teelo/web/templates/tournament_detail.html`
- Modify (optional for shared macro import ergonomics): `src/teelo/web/templates/partials/match_rows.html`

### Implementation approach
- Build page sections (top-to-bottom):
  1. Persistent header card with:
     - tournament name
     - tour badge using imported `circuit_bg/circuit_label`
     - city/country
     - surface + indoor/outdoor
     - draw size
     - edition number (count of editions)
     - prize money (if set)
     - recent champion + final score (if available)
     - date range
     - year selector `<select>`
  2. Tab bar `Matches | Draw` with teelo-lime active underline
     - Hide Draw tab entirely if `has_draw` false
  3. Tab content containers:
     - Matches container with loading state + desktop table + mobile card wrappers
     - Draw container with horizontal scroll wrapper + SVG overlay layer
  4. Persistent edition history section below tabs
- Attach page root dataset values for JS:
  - `data-tournament-code`, `data-year`, `data-has-draw`
- Load JS entry module at bottom:
  - `<script type="module" src=".../js/tournament_detail.js"></script>`

## Task 4. Matches Tab (Lazy-loaded + Filters)

### Files to create/modify
- Create: `src/teelo/web/static/js/tournament_detail.js`
- Create: `src/teelo/web/static/js/controllers/tournament_detail_controller.js`
- Optional create: `src/teelo/web/static/js/renderers/tournament_matches.js` (only if needed)

### Implementation approach
- Follow `player_detail.js` bootstrapping pattern:
  - `DOMContentLoaded` -> `initTournamentDetailPage()`.
- Controller responsibilities:
  - state: active tab, matches status (`completed`/`upcoming`), pagination
  - fetch matches endpoint lazily when Matches tab first opens
  - render returned `table_rows_html` and `cards_html`
  - round grouping headers inserted between row groups (based on API `groups` metadata)
  - show loading spinner + empty state correctly
- Filter chips:
  - Use existing `.filter-chip` CSS class and `.active` state class.
  - Clicking chip refetches matches from page 1.
- Keep rendering resilient:
  - If `table_rows_html`/`cards_html` missing, fallback to `buildFallbackTableRows`/`buildFallbackCards` in `renderers/matches.js`.

## Task 5. Draw/Bracket Tab (Columns + Connectors)

### Files to create/modify
- Modify: `src/teelo/web/static/js/controllers/tournament_detail_controller.js`
- Create: `src/teelo/web/static/js/renderers/bracket.js`
- Modify: `src/teelo/web/static/css/input.css` (new bracket utility classes)
- Regenerate: `src/teelo/web/static/css/styles.css` (via existing Tailwind build command)

### Implementation approach
- Lazy-load draw JSON only when Draw tab is activated first time.
- Render horizontal round columns in tournament order (`R128 -> F`).
- Each round column:
  - title row
  - match cards ordered by `draw_position`
  - each card has top/bottom player slots:
    - `seed + name`
    - score/status marker
    - winner styling (`font-bold`, teelo-lime accent)
    - placeholders for missing slots (`TBD` / `BYE`)
- SVG connectors:
  - Bracket math rule:
    - winner at position `p` feeds next round position `ceil(p/2)`
    - parent slots `2p-1` and `2p` connect into child slot `p`
  - Rendering sequence:
    1. render columns and cards
    2. read card anchor positions via `getBoundingClientRect`
    3. draw connector polylines in overlay SVG
    4. redraw on resize/tab open with debounce
- Mobile:
  - bracket container uses horizontal scrolling
  - keep connectors aligned to scrolled content via absolute SVG within same scroll canvas

## Task 6. Edition History Section

### Files to create/modify
- Modify: `src/teelo/web/templates/tournament_detail.html`
- Modify: `src/teelo/web/static/js/controllers/tournament_detail_controller.js`

### Implementation approach
- Section persists below tabs and loads independently from `/api/tournaments/{code}/editions`.
- Desktop table columns:
  - Year (linked), Champion, Runner-up, Score, Surface
- Mobile card variant mirrors current `player_detail` responsive strategy.
- Highlight current year row/card for orientation.
- Reuse existing surface color utilities/classes.
- Hook year selector and history links to canonical edition route format.

## Task 7. Navigation Links Across Site

### Files to create/modify
- Modify: `src/teelo/web/services/match_service.py`
- Modify: `src/teelo/web/templates/partials/match_rows.html`
- Modify: `src/teelo/web/static/js/renderers/matches.js`
- Optional updates where tournament names are rendered as plain text:
  - `src/teelo/web/static/js/controllers/player_detail_controller.js`
  - `src/teelo/web/templates/player_detail.html` (if server-rendered tournament names become links)

### Implementation approach
- Extend `serialize_match()` payload with tournament link fields:
  - `tournament_code`
  - `tournament_year` (prefer `TournamentEdition.year`, fallback from match date)
  - `tournament_url` = `/tournaments/{code}/{year}` when both values exist
- Update Jinja partial macro `render_table_rows` + `render_cards`:
  - tournament name clickable when `tournament_url` exists
- Update JS fallback renderer to emit linked tournament names similarly.
- This automatically propagates tournament links to home/matches/player pages where match rows/cards are reused.

## Edge Cases and Validation Checklist
- Unknown tournament code/year -> 404 page.
- Tournament exists but edition for year missing -> 404.
- Edition with no matches:
  - header still renders
  - Matches tab empty state
  - Draw tab hidden
  - history still available
- Edition with partial draw data:
  - Draw tab shown
  - missing card slots render `TBD`/`BYE`
- Final not present yet:
  - champion/score fields show placeholder `—`.
- Year selector:
  - changing year navigates full page route (not client-only state).

## Suggested Implementation Order
1. Router scaffold + HTML route + redirect route.
2. API endpoints (matches, draw, editions) with tests/manual curl checks.
3. Base tournament detail template scaffold + header/year selector/tab shell.
4. Matches tab controller + filter chips + macro-based table/cards.
5. Draw renderer + connector math + responsive behavior.
6. Edition history renderer.
7. Cross-site tournament links from match rows/cards.

## Acceptance Criteria
- `/tournaments/{code}` reliably redirects to latest available edition year.
- `/tournaments/{code}/{year}` renders header + tabs + edition history.
- Matches tab loads asynchronously and toggles `Completed/Upcoming` correctly.
- Draw tab only appears with draw data and shows valid bracket flow using `ceil(p/2)` linkage.
- Edition history lists all years with links and final-result metadata.
- Tournament names become clickable in shared match tables/cards across app surfaces.
