# Match Predictions Display — Design Spec

**Date:** 2026-03-15
**Status:** Approved

## Overview

Add prediction probabilities to match rows across the site, and create a new match detail page where users can view full prediction breakdowns with feature-by-feature comparison between players.

## Part 1: Match Row Prediction Display

### Data Changes

**`serialize_match()` in `match_service.py`:**
- Add `prediction_a` field (float 0.0–1.0) to the serialized payload
- The prediction must respect the existing side-swap logic (when display sides are swapped, `prediction_a` reflects the swapped Player A's probability)
- If `Match.prediction_a` is `None`, serialize as `null`

### Desktop Table Rows (`render_table_rows` macro)

- Add a new narrow column between the score column and the date column
- Display: two stacked percentage values — Player A's probability on top, Player B's below
- The higher probability is bolded with `text-teelo-dark font-semibold`; the lower is `text-content-faint`
- If prediction is `null`, show empty/dash
- Format: round to nearest integer percent (e.g., `62%` / `38%`)

### Mobile Cards (`render_cards` macro)

- Add prediction percentages between the meta line and the scoreboard section
- Compact inline format: `62% — 38%` with player context
- Higher probability side gets `font-semibold text-teelo-dark`; lower side `text-content-faint`

### Row Click Navigation

- Wrap each `<tr>` / card `<div>` to navigate to `/matches/{match_id}` on click
- Player name `<a>` tags and tournament name `<a>` tags retain their own navigation via `e.stopPropagation()` or by being real `<a>` elements that take precedence
- Add a small Lucide `chevron-right` icon as the last element in each row (new narrow column on desktop, right-aligned on mobile) as a visual affordance
- Row click uses JS `data-match-url` attribute + click handler (not wrapping in `<a>` to avoid nested link issues)

## Part 2: Match Detail Page

### Route

`GET /matches/{match_id}` — Server-rendered Jinja2 template

### API Endpoint

`GET /api/matches/{match_id}` — Returns JSON with:
```json
{
  "match": { /* full serialized match data */ },
  "features": {
    "feature_set": { "name": "...", "version": "...", "description": "..." },
    "groups": {
      "elo_core": {
        "display_name": "ELO Core",
        "features": [
          {"key": "elo", "display_name": "ELO Rating", "value_a": 2100.5, "value_b": 1950.3},
          {"key": "elo_diff", "display_name": "ELO Diff", "value": 150.2}
        ]
      },
      ...
    }
  }
}
```

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
- All sections expanded by default
- Section header: human-readable group name (derived from group key — e.g., `elo_core` → "ELO Core")
- Collapse/expand via Lucide chevron icon in header

**Inside each section — comparison table:**

| Feature | Player A | Player B |
|---------|----------|----------|
| ELO Rating | 2100 | 1950 |
| Surface ELO | 1980 | 2010 |

- Feature display names auto-generated from keys: strip `_a`/`_b` suffixes, replace underscores with spaces, title-case
- Features with `_a`/`_b` suffix pairs are merged into one row with values in respective columns
- Features with `_diff` suffix or no player suffix shown as single-value spanning row
- Value formatting: integers for whole numbers, 1-2 decimal places for floats, percentages where value is 0-1 and name contains "rate" or "ratio"
- Color coding: for paired features where higher = better (ELO, win rates, etc.), the higher value gets `text-status-success`, lower gets `text-status-danger`. Context and confidence features are displayed neutral (no coloring)

## Part 3: Dynamic Feature Handling

All feature display is driven by the data, not hardcoded:

1. **Feature grouping:** The API endpoint queries `MatchFeatures` for the match, gets the JSONB `features` dict. It determines groups by looking up the `FeatureSet.feature_definitions` or by using the feature registry's group mapping.

2. **Group display names:** A utility maps group keys to display names: `elo_core` → "ELO Core", `form` → "Form", `h2h` → "H2H", etc. Falls back to title-casing the key.

3. **Feature display names:** A utility function transforms feature keys:
   - Strip `_a` / `_b` suffixes
   - Replace `_` with spaces
   - Title-case
   - Special cases: "elo" → "ELO", "h2h" → "H2H"

4. **Pairing logic:** Features ending in `_a` are paired with corresponding `_b` features. The base name (without suffix) becomes the display name. Unpaired features (e.g., `h2h_sample_count`, `year`) are shown as single-value rows.

5. **No hardcoded feature lists.** When the model changes and new features appear in `MatchFeatures.features`, they automatically appear on the match detail page grouped and labeled correctly.

## Technical Notes

- Match detail page template extends `base.html`
- Collapsible sections use vanilla JS (toggle class, no framework needed)
- Feature data loaded server-side (no separate API call for the template-rendered page)
- The `/api/matches/{match_id}` JSON endpoint exists for potential future client-side use
- Prediction values on match rows come from the existing `serialize_match()` flow — no extra DB query needed since `prediction_a` is already on the `Match` model

## Files to Create/Modify

**Modify:**
- `src/teelo/web/services/match_service.py` — add `prediction_a` to serialization, add match detail serialization with features
- `src/teelo/web/templates/partials/match_rows.html` — prediction column, click handling, chevron
- `src/teelo/web/routers/matches.py` — new match detail route + API endpoint
- `src/teelo/web/static/js/controllers/matches_controller.js` — row click handler
- `src/teelo/web/static/css/input.css` — any new utility styles needed

**Create:**
- `src/teelo/web/templates/match_detail.html` — match detail page template
- `src/teelo/web/services/feature_display.py` — feature name/group formatting utilities
- `src/teelo/web/static/js/match_detail.js` — collapsible sections, any interactivity
