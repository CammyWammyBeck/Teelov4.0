# Feature Expansion Plan — Teelo v4.0

**Created**: 2026-03-21
**Status**: Draft — awaiting review

## Overview

Expand the v4 feature pipeline from ~95 features to ~160+ features. The core design principle is **relational features** — not "Player A is clutch" but "Player B struggles against clutch opponents." Features are conditional on the upcoming match context, not static player attributes.

All new feature groups follow the existing architecture: implement `FeatureGroup` ABC, register in `FeatureRegistry`, compute from `PlayerState` + `MatchContext` during chronological replay. Features are computed **before** the match outcome updates state (no leakage).

### Design Principle: Sample Size Companions

Every rate/win-rate feature must be accompanied by a **sample size feature** (e.g., `vs_clutch_matches_{a,b}` alongside `vs_clutch_win_rate_{a,b}`). This allows the model to learn how much to trust a rate — a 100% win rate over 2 matches means something very different from 100% over 50 matches. Rate features return `None` below minimum sample thresholds, but the count feature is always populated (even if 0), giving the model a continuous confidence signal.

---

## Tier 1 — Build Now (no data gaps)

### 1.1 Score Profile Features

**New group**: `score_profile`
**File**: `src/teelo/features/groups/score_profile.py`

Builds a rolling profile of how each player wins and loses, computed from the `MatchRecord` fields already populated by the engine (`games_won/lost`, `sets_won/lost`, `tiebreaks_played/won`, `deciding_set_played`, `straight_sets`, `close_match`).

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `tiebreak_win_rate_{a,b}` | `tiebreaks_won / tiebreaks_played` over last 64 matches | Yes |
| `tiebreaks_played_{a,b}` | Count of tiebreaks played in last 64 matches (sample size) | Yes |
| `deciding_set_win_rate_{a,b}` | Wins in deciding sets / deciding sets played, last 64 | Yes |
| `deciding_sets_played_{a,b}` | Count of deciding sets played in last 64 (sample size) | Yes |
| `comeback_rate_{a,b}` | Wins after losing first set / matches where first set lost, last 64 | Yes |
| `first_sets_lost_{a,b}` | Count of first sets lost in last 64 (sample size for comeback) | Yes |
| `straight_sets_win_rate_{a,b}` | `straight_sets AND won` / total wins, last 64 | Yes |
| `close_match_rate_{a,b}` | `close_match` / total matches, last 64 | Yes |
| `avg_games_margin_{a,b}` | Mean `(games_won - games_lost)` over last 32 matches | Yes |

**Minimum sample**: 5 matches (return `None` below that), consistent with existing `form.py` pattern.

**State changes needed**: `MatchRecord` already has all required fields. Need to add `first_set_lost` boolean to `MatchRecord` for comeback tracking (derived from `score_structured[0]`).

**New MatchRecord field**:
```python
first_set_lost: bool  # True if player lost the first set
```

Set in `engine.py` `_compute_score_summary()` alongside existing fields.

---

### 1.2 Clutch Profile — Trinomial Classification

**Extension to**: `score_profile` group (same file)

Each player gets a rolling **clutch score** derived from their score profile:

```
clutch_score = 0.4 * tiebreak_win_rate + 0.3 * deciding_set_win_rate + 0.3 * comeback_rate
```

Classify opponents into three buckets:
- **Clutch**: clutch_score > 0.55 (top ~25%)
- **Normal**: 0.40 ≤ clutch_score ≤ 0.55
- **Non-clutch**: clutch_score < 0.40 (bottom ~25%)

Thresholds are initial estimates — calibrate from actual data distribution after first backfill.

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `vs_clutch_win_rate_{a,b}` | Win rate against clutch-classified opponents, last 128 matches | Yes |
| `vs_clutch_matches_{a,b}` | Count of matches against clutch opponents, last 128 (sample size) | Yes |
| `vs_normal_clutch_win_rate_{a,b}` | Win rate against normal-classified opponents, last 128 matches | Yes |
| `vs_normal_clutch_matches_{a,b}` | Count of matches against normal opponents, last 128 (sample size) | Yes |
| `vs_non_clutch_win_rate_{a,b}` | Win rate against non-clutch opponents, last 128 matches | Yes |
| `vs_non_clutch_matches_{a,b}` | Count of matches against non-clutch opponents, last 128 (sample size) | Yes |
| `opponent_clutch_score_{a,b}` | The opponent's clutch_score at match time | Yes |

**State changes needed**: Add `clutch_score: float | None` to `PlayerState`. Updated after each match from the rolling score profile. Store in `MatchRecord` as `opponent_clutch_score` so it can be bucketed historically.

**New PlayerState field**:
```python
clutch_score: float | None = None  # rolling clutch classification score
```

**New MatchRecord field**:
```python
opponent_clutch_score: float | None  # opponent's clutch_score at match time
```

---

### 1.3 Surface Specialist Matchup — Trinomial Classification

**New group**: `specialist_matchup`
**File**: `src/teelo/features/groups/specialist_matchup.py`

Uses the existing `surface_elo` and `elo_current` on `PlayerState` to classify opponents:

```
specialist_score = surface_elo[ctx.surface] - elo_current
```

Classify opponents:
- **Specialist**: specialist_score > +75 (strongly prefer this surface)
- **Normal**: -75 ≤ specialist_score ≤ +75
- **Non-specialist**: specialist_score < -75 (weak on this surface)

Thresholds are initial estimates — calibrate from actual ELO distribution.

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `vs_specialist_win_rate_{a,b}` | Win rate against specialist opponents on current surface, last 128 | Yes |
| `vs_specialist_matches_{a,b}` | Count of matches against specialists, last 128 (sample size) | Yes |
| `vs_normal_specialist_win_rate_{a,b}` | Win rate against normal opponents on current surface, last 128 | Yes |
| `vs_normal_specialist_matches_{a,b}` | Count of matches against normal opponents, last 128 (sample size) | Yes |
| `vs_non_specialist_win_rate_{a,b}` | Win rate against non-specialist opponents on current surface, last 128 | Yes |
| `vs_non_specialist_matches_{a,b}` | Count of matches against non-specialists, last 128 (sample size) | Yes |
| `opponent_specialist_score_{a,b}` | The opponent's specialist_score at match time | Yes |
| `surface_specialist_gap_{a,b}` | Player's own specialist_score (surface_elo - overall_elo) | Yes |

**State changes needed**: None for PlayerState (already has `surface_elo` and `elo_current`). Add `opponent_specialist_score: float | None` to `MatchRecord`.

**New MatchRecord field**:
```python
opponent_specialist_score: float | None  # opponent's (surface_elo - elo) at match time
```

---

### 1.4 ELO Difference Matchup — Trinomial Classification

**New group**: `elo_matchup`
**File**: `src/teelo/features/groups/elo_matchup.py`

Uses the ELO difference at match time to classify historical opponents:

```
elo_diff = opponent_elo - player_elo  (positive = opponent was higher rated)
```

Classify opponents:
- **Higher-rated**: elo_diff > +100 (faced a clearly stronger opponent)
- **Same-rated**: -100 ≤ elo_diff ≤ +100
- **Lower-rated**: elo_diff < -100 (faced a clearly weaker opponent)

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `vs_higher_rated_win_rate_{a,b}` | Win rate against higher-rated opponents, last 128 | Yes |
| `vs_higher_rated_matches_{a,b}` | Count of matches against higher-rated, last 128 (sample size) | Yes |
| `vs_same_rated_win_rate_{a,b}` | Win rate against same-rated opponents, last 128 | Yes |
| `vs_same_rated_matches_{a,b}` | Count of matches against same-rated, last 128 (sample size) | Yes |
| `vs_lower_rated_win_rate_{a,b}` | Win rate against lower-rated opponents, last 128 | Yes |
| `vs_lower_rated_matches_{a,b}` | Count of matches against lower-rated, last 128 (sample size) | Yes |
| `upset_rate_{a,b}` | Win rate where `expected_win_prob < 0.35`, last 128 | Yes |
| `upset_opportunities_{a,b}` | Count of matches as heavy underdog, last 128 (sample size) | Yes |
| `hold_rate_{a,b}` | Win rate where `expected_win_prob > 0.65`, last 128 | Yes |
| `hold_opportunities_{a,b}` | Count of matches as heavy favourite, last 128 (sample size) | Yes |

**State changes needed**: `MatchRecord` already carries `opponent_elo` and `expected_win_prob`. No new fields needed.

---

### 1.5 Country Performance Features

**New group**: `country_performance`
**File**: `src/teelo/features/groups/country_performance.py`

How a player performs in the specific country of the upcoming match. NOT "home advantage" — captures any affinity a player has for a country (e.g., Boulter performing well in Australia due to personal connection, not nationality). Regional fallback provides a smoothed signal when country-specific data is sparse.

**Prerequisite**: Tournament `country_ioc` backfill (run before first feature backfill — see Data Enrichment section below).

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `country_win_rate_{a,b}` | Win rate in this tournament's country, all time | Yes |
| `country_matches_{a,b}` | Matches played in this country (sample size) | Yes |
| `country_delta_{a,b}` | `country_win_rate - overall_win_rate` (affinity signal) | Yes |
| `region_win_rate_{a,b}` | Win rate in this region (Europe/Asia-Pac/Americas/etc.) | Yes |
| `region_matches_{a,b}` | Matches played in this region (sample size) | Yes |
| `region_delta_{a,b}` | `region_win_rate - overall_win_rate` | Yes |
| `is_home_{a,b}` | Boolean: player nationality == tournament country | Yes |

**State changes needed**:
- Add `tournament_country_ioc: str | None` to `MatchContext` (resolved from tournament)
- Add `country_record: dict[str, tuple[int, int]]` to `PlayerState` (keyed by IOC → (wins, losses))
- Add `region_record: dict[str, tuple[int, int]]` to `PlayerState`
- Add `country_ioc: str | None` to `MatchRecord`

**Region mapping** (IOC code → region):
```python
REGIONS = {
    "Europe": {"GBR", "FRA", "ESP", "GER", "ITA", "SUI", "AUT", "NED", "BEL", "CZE", "SWE", "NOR", "DEN", "FIN", "POR", "GRE", "CRO", "SRB", "ROU", "HUN", "POL", "BUL", "UKR", "RUS", "GEO", "LTU", "LAT", "EST", "SVK", "SLO", "BIH", "MNE", "MKD", "ALB", "IRL", "SCO", "LUX", "AND", "MON", "CYP", "MLT", "ISR", ...},
    "Asia-Pacific": {"AUS", "JPN", "CHN", "KOR", "IND", "TPE", "THA", "VIE", "MAS", "SGP", "INA", "PHI", "NZL", "HKG", "MYA", "PAK", "SRI", "BAN", "UZB", "KAZ", "KGZ", ...},
    "Americas": {"USA", "ARG", "BRA", "CAN", "COL", "CHI", "MEX", "PER", "ECU", "URU", "VEN", "BOL", "PAR", "DOM", "PUR", "CRC", "GUA", "PAN", "JAM", "BAH", "TTO", ...},
    "Middle East & Africa": {"UAE", "QAT", "KSA", "RSA", "EGY", "MAR", "TUN", "NGR", "KEN", "ZIM", "BRN", "OMA", "JOR", "LBN", "TUR", ...},
}
```

Note: Turkey (`TUR`) is classified as Middle East here, but could be debated. Full mapping will be finalized during implementation.

---

### 1.6 Calendar & Context Features (renumbered from 1.5)

**Extension to**: existing `context` group (`src/teelo/features/groups/context.py`)

| Feature | Calculation |
|---------|-------------|
| `month_sin` | `sin(2π * month / 12)` — cyclical encoding |
| `month_cos` | `cos(2π * month / 12)` — cyclical encoding |
| `year_progress` | Day-of-year / 365 |

**State changes needed**: None. Derived from `ctx.match_date`.

---

### 1.7 Surface ELO Gap

**Extension to**: existing `elo` group (`src/teelo/features/groups/elo.py`)

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `surface_gap_{a,b}` | `surface_elo - overall_elo` for current surface | Yes |
| `off_surface_elo_{a,b}` | Average ELO across OTHER surfaces | Yes |

**State changes needed**: None. Derived from existing `PlayerState.surface_elo` dict and `elo_current`.

---

## Tier 1 Summary

| Group | New features | Data needed | Implementation |
|-------|-------------|-------------|----------------|
| score_profile + clutch | ~22 | Existing `MatchRecord` + 2 new fields | New group file + state changes |
| specialist_matchup | ~16 | Existing ELO data + 1 new `MatchRecord` field | New group file |
| elo_matchup | ~16 | Existing `MatchRecord.opponent_elo` | New group file |
| country_performance | ~14 | Tournament `country_ioc` (backfill first) | New group file + state changes |
| calendar (context ext.) | 3 | `ctx.match_date` | Edit existing group |
| surface_gap (elo ext.) | 4 | Existing `PlayerState` | Edit existing group |
| **Total** | **~75** | | |

Note: Feature counts include sample size companion features (e.g., `vs_clutch_matches` alongside `vs_clutch_win_rate`). All counts are ×2 for player a/b symmetry.

---

## Tier 1 Data Enrichment — Tournament Country Backfill

**Prerequisite for country_performance features. Must be completed before Phase 1 feature backfill.**

### Current state

| Level | Has country_ioc | Status |
|-------|----------------|--------|
| All levels | 100% | **COMPLETED** — 14,871 tournaments updated via `scripts/backfill_tournament_country.py` |

### Approach — COMPLETED 2026-03-21

1. Built `scripts/backfill_tournament_country.py` which:
   - Extracts city names from tournament names (handles all ITF/ATP/WTA/Challenger patterns)
   - Uses existing `geo.py` city→country lookup for known cities
   - Geocodes unknown cities via Nominatim (rate-limited 1 req/1.1s)
   - Caches results to `scripts/city_country_cache.json` (~2,500 mappings)
   - Batch-updates the database grouped by country (fast)
2. Result: **14,871 tournaments updated, 100% coverage** across all levels
   - Only 7 intentionally skipped (travelling events: Davis Cup, Laver Cup, ATP Challenger Tour Finals)
3. **Pipeline wired for new tournaments**: `geo.py:city_to_country()` reads from `scripts/city_country_cache.json` as the single source of truth (~2,600 city→country mappings). The scraping pipeline's `update_tournament_metadata()` (in `src/teelo/scrape/pipeline.py`) calls this automatically for new tournaments

---

## Tier 2 — Needs Data Enrichment

### 2.1 Handedness Matchup Features

**New group**: `handedness_matchup`
**File**: `src/teelo/features/groups/handedness_matchup.py`

Trinomial classification based on opponent handedness.

| Feature | Calculation | Per-player |
|---------|-------------|------------|
| `vs_lefty_win_rate_{a,b}` | Win rate against left-handed opponents | Yes |
| `vs_righty_win_rate_{a,b}` | Win rate against right-handed opponents | Yes |
| `vs_lefty_delta_{a,b}` | `vs_lefty_win_rate - overall_win_rate` | Yes |
| `opponent_is_lefty_{a,b}` | Binary: opponent is left-handed | Yes |

No trinomial here — it's binary (left vs right), but the delta captures how much a player's performance shifts.

**Requires**: `hand` on both players.

**State changes needed**:
- Add `opponent_hand: str | None` to `MatchRecord`
- Add `vs_hand_wins/losses: dict[str, tuple[int, int]]` to `PlayerState` (keyed by "Left"/"Right")

#### Data enrichment needed

| Source | Current coverage | Action |
|--------|-----------------|--------|
| ATP profiles | 48% at Masters, 38% at GS | Already scraped — this IS the current data |
| WTA profiles | **0%** | Need to add WTA profile scraper for `hand` field |
| ITF profiles | 0% for $35K+ levels | Low priority — these are WTA-sourced tournaments |

**Key gap**: WTA hand data is completely missing. Scrape from WTA player profile pages. ~750 active WTA players to cover.

---

### 2.3 Indoor/Outdoor Split

Not a feature group — a **data enrichment** that improves the existing surface context.

**Current state**: `indoor_outdoor` column exists on `tournaments` table but is 0% populated.

**Action**: Backfill from known tournament data. Most tournaments are consistently indoor or outdoor:
- All clay tournaments → outdoor (except rare exceptions)
- Grand Slams → known (AO outdoor, RG outdoor, Wimbledon outdoor, USO outdoor)
- Many hard court events are consistently one or the other

Once populated, add `surface_indoor` to the existing context features (it's already defined but always 0.0).

---

## Tier 3 — Major Collection Effort

### 3.1 Match Statistics Scraping

Serve %, ace rate, break point conversion/save rates. The gold standard for tennis analytics but requires building a new scraping pipeline.

**What to scrape**: ATP and WTA publish match stats for main tour events. Available for ATP 250+, WTA 250+, and Grand Slams. Not available for Challenger or ITF.

**Potential features** (if stats become available):
- `first_serve_pct_{a,b}` (rolling average)
- `second_serve_win_pct_{a,b}`
- `break_point_save_rate_{a,b}`
- `break_point_convert_rate_{a,b}`
- `ace_rate_{a,b}`
- `double_fault_rate_{a,b}`
- vs-opponent interaction versions of all the above

**Priority**: Deferred — high effort, and limited to top levels. The Tier 1 score-derived features capture some of the same signal (dominance, clutch) from data we already have.

### 3.2 Historical Odds Integration

Closing bookmaker odds for completed matches. Would give a "market consensus" feature.

**Potential sources**: Tennis-data.co.uk, OddsPortal historical data.

**Priority**: Deferred — requires external data integration and licensing consideration.

---

## Implementation Order

### Phase 0: Data Enrichment (prerequisite)

1. Backfill `tournaments.country_ioc` — manual fixes (3), city→country lookup (~250), ITF name parsing (~14,600)
2. Verify coverage with spot checks

### Phase 1: State Changes + Score Profile (foundation)

3. Add new fields to `MatchRecord` (`first_set_lost`, `opponent_clutch_score`, `opponent_specialist_score`, `country_ioc`)
4. Add new fields to `PlayerState` (`clutch_score`, country/region record dicts)
5. Add `tournament_country_ioc` to `MatchContext`
6. Update `engine.py` to populate new fields during replay
7. Implement `score_profile` group (includes clutch classification)
8. Implement `country_performance` group
9. Full backfill to validate

### Phase 2: Matchup Groups

10. Implement `specialist_matchup` group
11. Implement `elo_matchup` group
12. Extend `context` group with calendar features
13. Extend `elo` group with surface gap features
14. Full backfill + model retrain + evaluate

### Phase 3: Tier 2 Data Enrichment

15. Scrape WTA hand data
16. Implement `handedness_matchup` group
17. Backfill `indoor_outdoor` on tournaments
18. Full backfill + model retrain + evaluate

### Phase 4: Advanced (if Tier 1-2 show lift)

19. Match stats scraping pipeline
20. Historical odds integration

---

## Feature Count Projection

| Phase | New features | Running total |
|-------|-------------|---------------|
| Current baseline | — | ~95 |
| Phase 1 (score + clutch + country) | ~36 | ~131 |
| Phase 2 (specialist + elo matchup + calendar + elo gap) | ~39 | ~170 |
| Phase 3 (handedness + indoor/outdoor) | ~10 | ~180 |
| Phase 4 (stats + odds) | ~12+ | ~192+ |

---

## Calibration Notes

The trinomial thresholds (clutch: 0.40/0.55, specialist: ±75 ELO, elo_diff: ±100) are initial estimates. After the first backfill:

1. Run distribution analysis on all classification scores
2. Set thresholds at approximately 25th/75th percentiles
3. Verify bucket sizes are balanced enough for meaningful win rates (minimum ~20% of matches per bucket)
4. Consider making thresholds level-dependent (tighter at Grand Slam level where fields are stronger)

---

## Architecture Reference (for new agents)

Key files for implementing Phase 1+:

| File | Purpose |
|------|---------|
| `src/teelo/features/registry.py` | `FeatureGroup` ABC — all groups inherit from this. Defines `name`, `feature_names()`, `compute(state_a, state_b, ctx)` |
| `src/teelo/features/engine.py` | Chronological replay engine. Calls `registry.compute_all` with state BEFORE match outcome, then updates state. Batch flushes to DB every 5000 rows via `INSERT ... ON CONFLICT DO UPDATE` |
| `src/teelo/features/state.py` | `MatchRecord` (NamedTuple), `MatchContext` (dataclass), `PlayerState` (dataclass). Add new fields here |
| `src/teelo/features/groups/` | Existing feature groups: `context.py`, `elo.py`, `form.py`, `h2h.py`, `activity.py`, `opponent_quality.py`, `dominance.py`, `fatigue.py`, `tournament_history.py`, `confidence.py`, etc. |
| `src/teelo/db/models.py` | SQLAlchemy 2.0 models — `Tournament`, `Match`, `Player`, etc. |
| `src/teelo/utils/geo.py` | Geographic lookups. `city_to_country()` reads from `scripts/city_country_cache.json` (single source). `COUNTRY_TO_IOC` dict for IOC code resolution |
| `src/teelo/scrape/pipeline.py` | Scraping pipeline. `update_tournament_metadata()` auto-resolves country for new tournaments |

### Critical invariants
- **No leakage**: Features are computed from state BEFORE the current match outcome updates state
- **Sample size companions**: Every rate feature must have a count feature alongside it
- **Trinomial classification**: Opponent properties (clutch, specialist, elo tier) are bucketed into 3 categories, with win rates tracked per bucket
- **Minimum sample thresholds**: Rate features return `None` below threshold (typically 5 matches), count features always return a value
