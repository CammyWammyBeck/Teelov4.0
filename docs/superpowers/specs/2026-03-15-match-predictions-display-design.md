# Match Predictions Display — Design Spec

**Date:** 2026-03-15
**Status:** Approved

## Overview

Add prediction probabilities to match rows across the site, and create a new match detail page where users can view full prediction breakdowns with feature-by-feature comparison between players.

## Part 1: Match Row Prediction Display

### Data Changes

**`serialize_match()` in `match_service.py`:**
- Add `prediction_a` field (float 0.0–1.0) to the serialized payload
- Explicitly cast `Decimal` to `float` to avoid JSON serialization issues
- Side-swap handling: when `swap_display_sides` is true, serialize as `float(1.0 - match.prediction_a)` since the displayed "Player A" is actually the database Player B
- If `Match.prediction_a` is `None`, serialize as `null`

### Desktop Table Rows (`render_table_rows` macro)

- Add a new narrow column between the score column and the date column
- Display: two stacked percentage values — Player A's probability on top, Player B's below
- The higher probability is bolded with `text-teelo-dark font-semibold`; the lower is `text-content-faint`
- If prediction is `null`, show empty/dash
- Format: round to nearest integer percent (e.g., `62%` / `38%`)
- Add a `<th>` for the prediction column in the parent template's `<thead>`
- Add a final narrow column with a Lucide `chevron-right` icon (with corresponding `<th>`)

### Mobile Cards (`render_cards` macro)

- Add prediction percentages between the meta line and the scoreboard section
- Compact inline format: `62% — 38%` with player context
- Higher probability side gets `font-semibold text-teelo-dark`; lower side `text-content-faint`
- Add chevron-right icon right-aligned

### Row Click Navigation

- Wrap each `<tr>` / card `<div>` to navigate to `/matches/{match_id}` on click
- Add `data-match-url` attribute with the match URL, `cursor-pointer` class, `role="link"`, and `tabindex="0"` for accessibility
- Player name `<a>` tags and tournament name `<a>` tags retain their own navigation (real `<a>` elements take precedence; add `e.stopPropagation()` in the JS click handler)
- The chevron icon column should also be an actual `<a>` element for keyboard accessibility
- Row click handler attached via event delegation on the table/card container

### Fallback JS Renderers

- Update `src/teelo/web/static/js/renderers/matches.js` (`buildFallbackTableRows()` and `buildFallbackCards()`) to include the prediction column and chevron in the client-side fallback rendering path

## Part 2: Match Detail Page

### Route

`GET /matches/{match_id}` — Server-rendered Jinja2 template

Returns 404 page if match_id does not exist.

### Page Layout

#### 1. Prediction Hero (top, most prominent)

- Large centered section with a dual-color horizontal bar
- Player A name (linked) on left, Player B name (linked) on right
- Large percentage numbers (e.g., `62%` and `38%`)
- The bar width proportional to prediction split, using teelo-lime for the favored side
- If no prediction available, show "No prediction available" in muted text
- Model version shown as small muted text below

#### 2. Match Info Header

- Tournament name (linked to tournament page) with circuit badge
- Round, surface (color-coded), date
- Gender dot + level context (same styling as match rows)
- Score display for completed matches, with winner checkmark
- Player seeds if available

#### 3. Feature Comparison Sections

- One collapsible section per feature group
- First 3 groups expanded by default, rest collapsed (to avoid overwhelming long page with 11 groups)
- Section header: human-readable group name (derived from group key — e.g., `elo_core` → "ELO Core")
- Collapse/expand via Lucide chevron icon in header
- If no `MatchFeatures` exist for this match, show "No feature data available" message instead of empty/broken sections

**Inside each section — comparison table:**

| Feature | Player A | Player B |
|---------|----------|----------|
| ELO Rating | 2100 | 1950 |
| Surface ELO | 1980 | 2010 |

- Feature display names auto-generated from keys: strip `_a`/`_b` suffixes, replace underscores with spaces, title-case
- Features with `_a`/`_b` suffix pairs are merged into one row with values in respective columns
- Features with `_diff` suffix or no player suffix shown as single-value spanning row
- Value formatting: integers for whole numbers, 1-2 decimal places for floats, percentages where value is 0-1 and name contains "rate" or "ratio"
- Color coding: by default, paired features are colored (higher value = `text-status-success`, lower = `text-status-danger`). Groups where coloring doesn't apply (context, confidence) are identified by a `neutral_display` property on the `FeatureGroup` class (defaults to `False`). This keeps the logic dynamic — new groups default to colored unless explicitly marked neutral.

### Data Loading

- Feature data loaded server-side in the route handler (no separate API call)
- Query: `Match` joined with `MatchFeatures` + `FeatureSet`, selecting the **most recently computed** `MatchFeatures` record (by `computed_at`) when multiple feature sets exist for a match
- Explicit `joinedload` for `Match.player_a`, `Match.player_b`, `Match.tournament_edition.tournament`

## Part 3: Dynamic Feature Handling

All feature display is driven by the data, not hardcoded:

1. **Feature grouping:** Add a `grouped_features() -> dict[str, list[str]]` method to `FeatureRegistry` that returns `{group_name: [feature_names]}` by iterating registered `FeatureGroup` instances. The match detail route uses this to group the flat JSONB keys into sections. The registry is imported and instantiated at display time.

2. **Group display names:** A utility maps group keys to display names: `elo_core` → "ELO Core", `form` → "Form", `h2h` → "H2H", etc. Falls back to title-casing the key with underscores replaced by spaces.

3. **Feature display names:** A utility function transforms feature keys:
   - Strip `_a` / `_b` suffixes
   - Replace `_` with spaces
   - Title-case
   - Special cases: "elo" → "ELO", "h2h" → "H2H"

4. **Pairing logic:** Features ending in `_a` are paired with corresponding `_b` features. The base name (without suffix) becomes the display name. Unpaired features (e.g., `h2h_sample_count`, `year`) are shown as single-value rows.

5. **No hardcoded feature lists.** When the model changes and new features appear in `MatchFeatures.features`, they automatically appear on the match detail page grouped and labeled correctly.

6. **Neutral display marking:** `FeatureGroup` base class gets an optional `neutral_display` property (default `False`). `ContextFeatures` and `ConfidenceFeatures` override to return `True`. The display utility checks this to skip color coding for those groups.

## Technical Notes

- Match detail page template extends `base.html`
- Collapsible sections use vanilla JS (toggle class, no framework needed)
- Prediction values on match rows come from the existing `serialize_match()` flow — no extra DB query needed since `prediction_a` is already on the `Match` model

## Files to Create/Modify

**Modify:**
- `src/teelo/web/services/match_service.py` — add `prediction_a` to serialization (with Decimal→float cast and swap logic), add match detail data builder
- `src/teelo/web/templates/partials/match_rows.html` — prediction column, click handling, chevron
- `src/teelo/web/templates/matches.html` (or wherever `<thead>` lives) — add `<th>` for prediction and chevron columns
- `src/teelo/web/routers/matches.py` — new match detail route
- `src/teelo/web/static/js/controllers/matches_controller.js` — row click handler via event delegation
- `src/teelo/web/static/js/renderers/matches.js` — update fallback renderers with prediction column and chevron
- `src/teelo/web/static/css/input.css` — any new utility styles needed
- `src/teelo/features/registry.py` — add `grouped_features()` method to `FeatureRegistry`
- `src/teelo/features/groups/` — add `neutral_display` property to base `FeatureGroup`, override in `context.py` and `confidence.py`

**Create:**
- `src/teelo/web/templates/match_detail.html` — match detail page template
- `src/teelo/web/services/feature_display.py` — feature name/group formatting utilities
- `src/teelo/web/static/js/match_detail.js` — collapsible sections, any interactivity
