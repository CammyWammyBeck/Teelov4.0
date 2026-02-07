# Plan: Matches Page with Filtering, API, and Infinite Scroll

## Summary

Build a dedicated `/matches` page with a rich filtering system, backed by a JSON API (`/api/matches`). The home page keeps its current "recent matches" preview and links through to the full matches page. Filters use an inline pill/chip UI for common filters with a "More Filters" slide-out drawer for less-used options. Date filtering uses quick presets plus a custom date picker. Filter state is synced to URL query params for shareability. Matches load via infinite scroll.

---

## Architecture Overview

```
User clicks filter chip / types player name / scrolls down
        |
        v
  Vanilla JS (matches.js)
        |
        |-- Updates URL query params (history.replaceState)
        |-- Fetches GET /api/matches?tour=ATP&surface=Hard&page=1
        |
        v
  FastAPI JSON endpoint (/api/matches)
        |
        |-- Parses & validates query params
        |-- Builds SQLAlchemy query with filters
        |-- Returns paginated JSON response
        |
        v
  JS renders match rows into the table
```

**Key files to create/modify:**

| File | Action | Purpose |
|------|--------|---------|
| `src/teelo/web/main.py` | Modify | Add `/matches` route, `/api/matches` endpoint, `/api/players/search` endpoint |
| `src/teelo/web/templates/matches.html` | Create | Full matches page with filter UI |
| `src/teelo/web/templates/home.html` | Modify | Replace dummy data with real data, add "View all" link to `/matches` |
| `src/teelo/web/static/js/matches.js` | Create | Filter controller, infinite scroll, player autocomplete |
| `src/teelo/web/static/css/input.css` | Modify | Add filter chip styles, drawer styles, scrollbar hiding |

---

## Step 1: Build the `/api/matches` JSON endpoint

**File:** `src/teelo/web/main.py`

Add a new route that returns paginated, filterable match data as JSON.

### Query Parameters

| Param | Type | Example | Description |
|-------|------|---------|-------------|
| `tour` | str (comma-separated) | `ATP,WTA` | Filter by tour(s) |
| `surface` | str (comma-separated) | `Hard,Clay` | Filter by surface(s) |
| `level` | str (comma-separated) | `Grand Slam,Masters 1000` | Filter by tournament level(s) |
| `round` | str (comma-separated) | `F,SF,QF` | Filter by round(s) |
| `status` | str (comma-separated) | `completed,retired` | Filter by match status (default: completed + retired + walkover + default) |
| `player` | str | `Sinner` | Search player name (matches either player_a or player_b) |
| `player_id` | int | `42` | Exact player ID filter |
| `player_a_id` | int | `42` | Head-to-head: Player A |
| `player_b_id` | int | `15` | Head-to-head: Player B |
| `tournament` | str | `Australian Open` | Tournament name search |
| `date_from` | str (YYYY-MM-DD) | `2024-01-01` | Start date |
| `date_to` | str (YYYY-MM-DD) | `2024-12-31` | End date |
| `date_preset` | str | `7d`, `30d`, `ytd`, `2024`, `2023` | Quick date presets |
| `page` | int | `1` | Page number (1-indexed) |
| `per_page` | int | `50` | Results per page (max 100) |

### Response Format

```json
{
  "matches": [
    {
      "id": 1234,
      "tour": "ATP",
      "tournament_name": "Australian Open",
      "tournament_level": "Grand Slam",
      "surface": "Hard",
      "round": "F",
      "player_a": {"id": 42, "name": "J. Sinner", "seed": 1},
      "player_b": {"id": 15, "name": "A. Zverev", "seed": 2},
      "score": "6-3 7-6 6-3",
      "winner_id": 42,
      "status": "completed",
      "match_date": "2025-01-26",
      "year": 2025
    }
  ],
  "total": 5432,
  "page": 1,
  "per_page": 50,
  "has_more": true
}
```

### Implementation Details

- Join `matches` -> `tournament_editions` -> `tournaments` for tour/level/surface filtering
- Join `matches` -> `players` (both player_a and player_b) for player name search
- Use `ilike` for player name search (case-insensitive partial match)
- For head-to-head mode: filter where (player_a_id = X AND player_b_id = Y) OR (player_a_id = Y AND player_b_id = X)
- Order by `match_date DESC NULLS LAST` (most recent first)
- Use a window function or separate count query for `total`
- Use `.offset().limit()` for pagination
- Date presets resolve server-side: `7d` -> last 7 days from today, `2024` -> full year 2024, `ytd` -> Jan 1 of current year to today
- Default status filter includes: `completed`, `retired`, `walkover`, `default` (i.e., all finished matches)

---

## Step 2: Build the `/api/players/search` endpoint

**File:** `src/teelo/web/main.py`

For the player autocomplete, add a lightweight search endpoint.

### Query Parameters

| Param | Type | Example | Description |
|-------|------|---------|-------------|
| `q` | str | `Sinn` | Search query (min 2 chars) |
| `limit` | int | `8` | Max results (default 8) |

### Response Format

```json
{
  "players": [
    {"id": 42, "name": "Jannik Sinner", "nationality": "ITA"},
    {"id": 198, "name": "Jack Sinners", "nationality": "AUS"}
  ]
}
```

### Implementation Details

- Search `players.canonical_name` with `ilike` (`%query%`)
- Also search `player_aliases.alias` with `ilike` for broader matching
- Deduplicate results (a player might match on both name and alias)
- Order by: exact prefix matches first, then alphabetical
- Debounced on client side (300ms)

---

## Step 3: Build the `/matches` page template

**File:** `src/teelo/web/templates/matches.html`

### Layout Structure

```
+-----------------------------------------------------+
|  Page header: "Match History"                        |
|  Subtitle: "Browse and filter results across tours"  |
+-----------------------------------------------------+
|  INLINE FILTER CHIPS (always visible, scrollable)    |
|                                                      |
|  Tour:    [ATP] [WTA] [Challenger] [ITF]            |
|  Surface: [Hard] [Clay] [Grass]                      |
|  Date:    [7d] [30d] [This Year] [2024] [2023] [Custom...] |
|                                                      |
|  Player search: [Search player...]   [More Filters]  |
|                                                      |
|  Active filters: "ATP, Hard, Last 30 days"  [Clear all] |
+-----------------------------------------------------+
|  MATCH TABLE (same design as home page)              |
|  ... rows loaded from API ...                        |
|  ... infinite scroll loads more ...                  |
|                                                      |
|  Loading spinner at bottom when fetching more        |
|  "No matches found" empty state when filters too narrow |
+-----------------------------------------------------+
|  SLIDE-OUT DRAWER (triggered by "More Filters")      |
|                                                      |
|  Level:  [Grand Slam] [Masters 1000] [ATP 500] ...  |
|  Round:  [F] [SF] [QF] [R16] [R32] [R64]           |
|  Status: [Completed] [Retired] [Walkover]           |
|  Year:   [2025] [2024] [2023] [2022] [2021] [2020] |
|  Tournament search: [Search tournament...]           |
|  Head-to-Head toggle                                 |
|    Player A: [autocomplete]                          |
|    Player B: [autocomplete]                          |
|                                                      |
|  [Apply Filters]                                     |
+-----------------------------------------------------+
```

### Filter Chip Design

Chips are toggle buttons with distinct colors per category:

| Category | Colors (active) | Colors (inactive) |
|----------|----------------|-------------------|
| Tour: ATP | `bg-[#002865] text-white` | `bg-white text-gray-500 border border-gray-200` |
| Tour: WTA | `bg-[#E30066] text-white` | same inactive |
| Tour: Challenger | `bg-[#006B3F] text-white` | same inactive |
| Tour: ITF | `bg-gray-600 text-white` | same inactive |
| Surface: Hard | `bg-blue-500 text-white` | same inactive |
| Surface: Clay | `bg-orange-500 text-white` | same inactive |
| Surface: Grass | `bg-green-600 text-white` | same inactive |
| Date presets | `bg-teelo-dark text-white` | same inactive |

Chips have a subtle scale animation on click and a checkmark icon when active.

### Active Filter Summary

Below the chips, a summary line shows applied filters as removable tags:
```
Showing: ATP · Hard · Last 30 days · Player: Sinner    [Clear all]
```

Each tag has an X to remove that individual filter.

### Mobile Adaptation

- Filter chips become a horizontally scrollable row (hiding scrollbar with CSS)
- "More Filters" button becomes full-screen modal instead of drawer
- Match table switches to card layout (reusing the existing mobile card design from home.html)
- Player search moves into the drawer on mobile

---

## Step 4: Build the JavaScript filter controller

**File:** `src/teelo/web/static/js/matches.js`

### Module Structure

```javascript
// State management
const filterState = {
  tour: [],           // ['ATP', 'WTA']
  surface: [],        // ['Hard']
  level: [],          // ['Grand Slam']
  round: [],          // ['F', 'SF']
  status: [],         // ['completed', 'retired', 'walkover', 'default']
  player: '',         // free text search
  player_id: null,    // selected from autocomplete
  player_a_id: null,  // head-to-head
  player_b_id: null,  // head-to-head
  tournament: '',     // tournament name search
  date_from: '',      // YYYY-MM-DD
  date_to: '',        // YYYY-MM-DD
  date_preset: '',    // '7d', '30d', 'ytd', '2024'
  page: 1,
};
```

### Key Functions

1. **`initFromURL()`** - Parse URL query params into filterState on page load
2. **`syncToURL()`** - Push filterState to URL via `history.replaceState()`
3. **`fetchMatches(append)`** - GET `/api/matches` with current filters; if `append=true`, add to existing rows (infinite scroll)
4. **`renderMatches(matches, append)`** - Build match row HTML and insert into the table; desktop table rows + mobile card layout
5. **`toggleChip(category, value)`** - Toggle a filter chip on/off, reset page to 1, re-fetch
6. **`setupInfiniteScroll()`** - IntersectionObserver on a sentinel element at bottom of matches list
7. **`setupPlayerAutocomplete(inputEl, callback)`** - Debounced search with dropdown results
8. **`openDrawer()` / `closeDrawer()`** - Slide-out filter drawer
9. **`clearAllFilters()`** - Reset everything, re-fetch
10. **`renderActiveSummary()`** - Update the active filters summary line with removable tags
11. **`handlePopState()`** - Listen for browser back/forward, re-read URL, re-fetch

### Infinite Scroll

- Place a `<div id="scroll-sentinel">` after the match rows container
- `IntersectionObserver` watches it; when it becomes visible, increment `page` and `fetchMatches(append=true)`
- Show a loading spinner inside the sentinel during fetch
- Hide sentinel / stop observing when `has_more` is false from API response
- When filters change, clear all rows, reset page to 1, re-enable observer

### Player Autocomplete

- `<input>` with 300ms debounce via `setTimeout`/`clearTimeout`
- On each input event (after debounce), fetch `/api/players/search?q=...`
- Show dropdown below input with player names + nationality IOC code
- Clicking a player sets `player_id` (or `player_a_id`/`player_b_id` for H2H) and displays their name as a chip
- ESC or clicking outside closes dropdown
- Minimum 2 characters before searching

### URL Sync

- Array filters use comma separation: `?tour=ATP,WTA&surface=Hard`
- Single-value filters: `?player_id=42&date_preset=30d`
- On page load, `initFromURL()` parses URL params, applies them to chip states, and triggers initial fetch
- On filter change, `syncToURL()` updates URL via `history.replaceState` (no page reload)
- `popstate` event listener handles browser back/forward

---

## Step 5: Update the home page

**File:** `src/teelo/web/templates/home.html`

- Remove the hardcoded `dummy_matches` Jinja2 variable
- The server-side query in `main.py` already fetches real matches and passes them as `matches` - wire the template to use this data
- Access tournament name via `m.tournament_edition.tournament.name`, surface via `m.tournament_edition.tournament.surface` or `m.tournament_edition.surface`, etc.
- Determine tour badge color from `m.tournament_edition.tournament.tour`
- Determine surface color from the surface value
- Determine winner by comparing `m.winner_id` to `m.player_a_id`
- Update "View all match history" link at the bottom to point to `/matches`
- Keep the stat cards (can be wired to real counts later)

---

## Step 6: Add CSS for filters and drawer

**File:** `src/teelo/web/static/css/input.css`

Add styles for:
- `.filter-chip` - Base chip: `px-3 py-1.5 rounded-full text-xs font-semibold cursor-pointer transition-all duration-150 select-none`
- `.filter-chip:hover` - Subtle lift/scale
- `.filter-chip.active` - Active state (category-specific colors applied via data attributes or individual classes)
- `.filter-drawer-overlay` / `.filter-drawer` - Slide-out panel (reuse the mobile menu pattern already in `input.css`)
- `.hide-scrollbar` - `overflow-x: auto; -ms-overflow-style: none; scrollbar-width: none;` and `::-webkit-scrollbar { display: none; }`
- `.autocomplete-dropdown` - Absolute positioned below input, white bg, shadow, max-height with scroll
- `.autocomplete-item` - Hover state, padding, cursor pointer
- `.loading-spinner` - Simple CSS spinner animation
- `.filter-summary-tag` - Inline removable tags with X button
- `.empty-state` - "No matches found" centered message

---

## Step 7: Custom date picker

For the "Custom" date preset chip:
- Clicking "Custom" toggles visibility of a small inline row with two `<input type="date">` fields (From / To)
- Uses native browser date pickers (good cross-browser/mobile support, zero dependencies)
- "Apply" button next to the date fields triggers the filter
- When a custom date range is active, the "Custom" chip shows as active with the date range in the summary
- Clearing the custom date range hides the date inputs and removes the date filter

---

## Implementation Order

1. **API endpoint** (`/api/matches`) - Can test independently with curl/browser
2. **Player search endpoint** (`/api/players/search`) - Needed for autocomplete
3. **Matches page template** (`matches.html`) - Static HTML structure with filter chip placeholders
4. **CSS additions** (`input.css`) - Filter chip styles, drawer, autocomplete dropdown
5. **JavaScript controller** (`matches.js`) - Wire everything together: chips, API calls, rendering, infinite scroll, autocomplete, URL sync
6. **Home page update** (`home.html`) - Replace dummy data with real DB data, add `/matches` link
7. **Testing & polish** - Verify all filters work, mobile responsiveness, empty states, edge cases

---

## Scope Boundaries

### In scope
- `/api/matches` JSON endpoint with all listed filters
- `/api/players/search` autocomplete endpoint
- `/matches` page with chip filters, slide-out drawer, infinite scroll, player autocomplete
- Home page wired to real data with link to `/matches`
- URL query param sync (shareable/bookmarkable filter states)
- Mobile responsive design (scrollable chips, card layout, full-screen drawer)
- Head-to-head filter mode
- Active filter summary with removable tags

### Out of scope (future work)
- Column sorting (currently fixed to most recent first)
- Match detail page (clicking a match row)
- Export/download filtered results
- Saved filter presets
- Advanced match statistics display in the table
- Tournament-specific pages
- ELO ratings display (depends on Phase 2 ELO implementation)
