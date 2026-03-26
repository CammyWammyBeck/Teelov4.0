# Hypothetical Matchup Page — Design Spec

**Date:** 2026-03-26
**Status:** Draft

## Overview

A page where users enter two players and optional match context (surface, level, round, date, etc.) to get a model prediction and full feature breakdown — the same view used for real match predictions. Includes three prerequisite infrastructure changes: ELO unification, PlayerState snapshotting, and incremental feature engine.

## Prerequisites

### P1: ELO Unification

The feature engine currently computes its own ELO values in-memory while also reading persisted ELO from the ELO updater's tables. This duplicates work and risks divergence.

**Change:** Remove ELO computation from the feature engine. Make the ELO updater the single source of truth.

- Remove from `PlayerState`: `elo_current`, `elo_peak`, `surface_elo`, `surface_elo_peak`, `elo_history`
- Feature groups (`EloCoreFeatures`, `EloHistoryFeatures`, `EloVarianceFeatures`) read ELO values from the ELO updater's persisted tables (`player_elo_states`, `player_surface_elo_states`, `match_surface_elo_snapshots`) instead of `PlayerState` fields
- ELO data is loaded once at engine start and passed into feature computation via `MatchContext` or a separate ELO lookup object
- Pipeline ordering unchanged: ELO updater runs before feature engine

### P2: PlayerState Snapshotting

Currently `PlayerState` is rebuilt from scratch every engine run by replaying all historical matches. We add persistence so states can be saved and restored.

**New tables:**

`player_feature_states` — current state per player (resume point):
| Column | Type | Notes |
|--------|------|-------|
| player_id | int | PK (composite) |
| feature_set_id | int | PK (composite), FK to feature_sets |
| temporal_order | int | Last processed match's temporal_order |
| state_json | JSONB | Serialized PlayerState |
| updated_at | timestamp | |

`player_feature_snapshots` — state at each match (historical lookups):
| Column | Type | Notes |
|--------|------|-------|
| id | serial | PK |
| player_id | int | FK to players |
| match_id | int | FK to matches |
| feature_set_id | int | FK to feature_sets |
| temporal_order | int | For date-range queries |
| state_json | JSONB | Serialized PlayerState |
| created_at | timestamp | |
| | | Unique on (player_id, match_id, feature_set_id) |

**Serialization:** `PlayerState.to_dict()` / `PlayerState.from_dict()` class methods handling: deque→list, NamedTuple→dict, date→ISO string, defaultdict→dict, int dict keys→string keys. Round-trip tested.

**Write path:** After `state.update()` in the engine loop, batch-insert snapshots using the same batching pattern as feature row writes (every 5000 matches). Only write snapshots for matches not yet snapshotted (gate on existing snapshot IDs, same as existing feature watermark).

**Storage estimate:** ~200K snapshot rows for the initial backfill (2 players × ~100K historical matches). Each JSONB blob is 5-20KB (the 1024-entry match deque is the largest field). Total ~2-4GB. Index on `(player_id, temporal_order, feature_set_id)` for efficient date-range lookups. No pruning needed — snapshots are append-only and the growth rate is small (~200-600 rows/day).

### P3: Incremental Feature Engine

Modeled on the ELO updater's incremental pattern.

**Resume flow:**
1. Load `player_feature_states` for all players → deserialize into `PlayerState` dict
2. Find resume point: `max(temporal_order)` across all player states
3. Query matches with `temporal_order > resume_point` (instead of all matches)
4. Process new matches: compute features, update states, write snapshots
5. Batch-write: feature rows + current states + per-match snapshots

**Full rebuild trigger:** If feature set schema version changes (new feature groups added), discard all snapshots for that feature set and rebuild from scratch. A `schema_version` column on `feature_sets` flags this.

**Backfill detection:** If a new match has `temporal_order < resume_point` (e.g., retroactive data correction), fall back to full rebuild from scratch (temporal_order = 0). Partial rollback (rebuilding from a mid-point) would require identifying all affected player states and is not worth the complexity — full rebuilds are infrequent and the incremental path handles the normal case.

## Hypothetical Matchup Page

### Route

`GET /matchup` — renders the form page.

### Form Inputs

| Field | Type | Required | Default |
|-------|------|----------|---------|
| Player A | Autocomplete (existing `/api/players/search`) | Yes | — |
| Player B | Autocomplete | Yes | — |
| Date | Date picker | No | Today (uses current snapshots) |
| Surface | Dropdown: Hard, Clay, Grass, Carpet | No | None |
| Tournament Level | Dropdown: Grand Slam, Masters 1000, ATP 500, ATP 250, Challenger, ITF | No | None |
| Tour | Dropdown: ATP, WTA | No | Auto-inferred from player gender |
| Round | Dropdown: F, SF, QF, R16, R32, R64, R128 | No | None |
| Seed A | Number | No | None |
| Seed B | Number | No | None |
| Tournament Country | Autocomplete/dropdown (IOC codes) | No | None |

**Behavior:**
- When a player is selected, their current ELO appears next to their name
- Tour auto-set from player gender, can be overridden
- All optional fields left blank → None passed to feature computation
- XGBoost handles NaN natively; features depending on missing context produce NaN

### Prediction Flow (Background Task with Polling)

**Submit:** `POST /api/matchup/predict`

Request body:
```json
{
  "player_a_id": 123,
  "player_b_id": 456,
  "date": "2026-01-15",
  "surface": "Clay",
  "level_code": "GS",
  "tour": "ATP",
  "round": "F",
  "seed_a": null,
  "seed_b": null,
  "country_ioc": "FRA"
}
```

Response: `{"task_id": "abc123"}`

**Task execution (background thread):**
1. Load player feature snapshots — if a date is specified, join `player_feature_snapshots` through `matches` to find the snapshot with the highest `temporal_order` where `match.match_date <= requested_date`. If date is today or omitted, load from `player_feature_states` (current state)
2. Load ELO values — from `player_elo_states` / `player_surface_elo_states` for the same point in time
3. Build `MatchContext` from form inputs (None for omitted fields)
4. Compute features — `registry.compute_all(state_a, state_b, ctx)`
5. Run inference — XGBoost model (cached in app state at startup), single-row DataFrame, `predict_proba()`
6. Apply A/B averaging — same symmetric prediction as the real pipeline (original + swapped orientation, averaged)
7. Store result in in-memory dict keyed by task ID

**Poll:** `GET /api/matchup/predict/{task_id}`

Response variants:
- `{"status": "pending"}`
- `{"status": "complete", "result": {...}}`
- `{"status": "error", "message": "..."}`

**Task store:** In-memory dict with 30-minute TTL cleanup. No Redis or database needed.

### Page States (Single Page, No Navigation)

**State 1 — Form:**
- Dark-themed form matching site design (teelo-dark background, teelo-lime accents)
- Player autocomplete fields with ELO preview badges
- Context fields in a responsive grid (3-column on desktop, stacked on mobile)
- "Predict Match" button (teelo-lime, full-width)

**State 2 — Predicting:**
- Form collapses/slides up
- Player names displayed prominently with "vs" between them
- Animated loading indicator (pulsing bars in teelo-lime)
- Rotating status text cycling through feature groups: "Crunching 97 features...", "Analyzing form", "Surface ELO", "Head-to-head", "Score profiles"

**State 3 — Result:**
- Prediction hero: player names, ELO ratings, split prediction bar (teelo-lime for favored side)
- Context banner: "Clay · Grand Slam · Final · as of 15 Jan 2026" (shows specified inputs, omits blanks)
- "Hypothetical Matchup" label instead of match status
- Feature group breakdown — collapsible sections, same rendering as match detail page
- Uses `build_feature_groups()` and `format_feature_value()` from existing `feature_display.py`
- "Modify Inputs & Re-predict" button at bottom re-expands the form

**Hidden compared to real match detail:**
- No score section
- No winner highlight
- No post-match ELO change
- No tournament name/link

### Template Strategy

Extract prediction display from `match_detail.html` into a `_match_prediction.html` Jinja2 partial. The real match detail page includes this partial server-side as before.

The matchup page is a new `matchup.html` template containing the form, loading state, and an empty result container. **Result rendering is fully client-side:** the polling response returns JSON (prediction value, player data, feature groups), and JS builds the result DOM using the same visual structure as the Jinja2 partial. This avoids a second server round-trip for HTML rendering and keeps the form→loading→result transition smooth. The matches page JS already follows this pattern (client-side match row rendering).

### Model Loading

XGBoost model + feature metadata loaded once at app startup and cached as FastAPI app state. Same approach as the existing predictor module. The `/api/matchup/predict` endpoint reuses this cached model.

## Out of Scope

- Unknown/free-text players (only database players via autocomplete)
- Live preview / real-time updates as form fields change (v2 consideration)
- Shareable URLs for predictions
- Comparison of multiple hypothetical scenarios side by side
