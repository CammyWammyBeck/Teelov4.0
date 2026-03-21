# Phase 1 Feature Expansion — Design Spec

**Created**: 2026-03-21
**Status**: Draft
**Scope**: Plan steps 3–9 (state changes + score_profile + country_performance + calendar + surface gap). Specialist matchup and ELO matchup are Phase 2 (steps 10–14).

## Goal

Expand the feature pipeline from ~158 to ~230+ features by implementing Phase 1 of the feature expansion plan: score profile (absorbing dominance), clutch matchup, country performance, calendar context, and surface gap features.

Note: The original plan estimated ~36 new features for Phase 1, but the decision to keep both 8-match and 64-match windows (absorbing dominance rather than replacing it) increases this to ~77 new features. This is intentional — more features enables better feature selection later.

## Design Decisions

- **Dominance merge**: The existing `dominance.py` group is absorbed into `score_profile.py`. Both 8-match and 64-match windows are kept (short = recent form, long = career tendency). `dominance.py` is deleted.
- **Sample size companions**: Rate features where the denominator is a subset of matches (tiebreaks, deciding sets, comeback opportunities) get explicit count companions. Rate features where the denominator is simply "matches in window" do not need a separate companion — the window size is implicit.
- **Clutch classification**: Computed from 64-window score profile rates, stored on `PlayerState`, snapshotted to `MatchRecord` as `opponent_clutch_score` for historical bucketing.

---

## 1. State Changes

### 1.1 MatchRecord (NamedTuple) — 4 new fields

**Must be appended after `close_match` (the last existing field) to preserve NamedTuple positional ordering.**

```python
first_set_lost: bool = False           # True if player lost the first set
opponent_clutch_score: float | None = None   # opponent's clutch_score at match time
opponent_specialist_score: float | None = None  # opponent's (surface_elo - elo) at match time
country_ioc: str | None = None         # tournament country IOC code
```

- `first_set_lost`: derived from `score_structured[0]` in engine's score parsing
- `opponent_clutch_score`: read from opponent's `PlayerState.clutch_score` at match time
- `opponent_specialist_score`: computed as `opponent.surface_elo[surface] - opponent.elo_current` at match time (Phase 2 uses this, but cheap to populate now)
- `country_ioc`: passed through from `Tournament.country_ioc`

### 1.2 PlayerState (dataclass) — 3 new fields

```python
clutch_score: float | None = None                    # rolling clutch classification score
country_record: dict[str, tuple[int, int]] = field(default_factory=dict)  # {ioc: (wins, losses)}
region_record: dict[str, tuple[int, int]] = field(default_factory=dict)   # {region: (wins, losses)}
```

- `clutch_score`: recomputed after each state update from the last 64 matches' tiebreak/deciding-set/comeback rates
- `country_record` / `region_record`: updated in `PlayerState.update()` when `MatchRecord.country_ioc` is not None

**Region resolution in `update()`**: Import `ioc_to_region()` from `src/teelo/utils/geo.py` (see section 3.1 for the mapping). This avoids circular dependencies — the mapping lives in the utility layer, not in a feature group.

### 1.3 MatchContext (dataclass) — 3 new fields

```python
tournament_country_ioc: str | None = None   # from Tournament.country_ioc
player_a_nationality: str | None = None     # from Player.nationality_ioc
player_b_nationality: str | None = None     # from Player.nationality_ioc
```

### 1.4 Engine Changes

**`_load_matches()`**:
- Add `Tournament.country_ioc.label("tournament_country_ioc")` to the SELECT columns (labeled to avoid ambiguity with any other `country_ioc` column)

**Player nationality loading**:
- Import `Player` model into `engine.py`
- At the start of `run()`, before the main loop, load all nationalities: `select(Player.id, Player.nationality_ioc)` → build `{player_id: nationality_ioc}` dict
- Store as instance variable `self._nationalities`

**`MatchContext` construction**:
- Set `tournament_country_ioc` from `row.tournament_country_ioc`
- Set `player_a_nationality` and `player_b_nationality` from `self._nationalities.get(row.player_a_id)`

**Score parsing** — extend to compute `first_set_lost`:
```python
# After existing score parsing
first_set_lost_a = False
first_set_lost_b = False
if score_structured and len(score_structured) > 0:
    first_set = score_structured[0]
    set_a = first_set.get("a", 0)
    set_b = first_set.get("b", 0)
    first_set_lost_a = set_a < set_b
    first_set_lost_b = set_b < set_a
```

**MatchRecord construction** — add new fields:
```python
record_a = MatchRecord(
    ...,
    first_set_lost=first_set_lost_a,
    opponent_clutch_score=state_b.clutch_score,
    opponent_specialist_score=_specialist_score(state_b, surface),
    country_ioc=ctx.tournament_country_ioc,
)
```

**Post-update clutch recompute**:
After calling `state.update()`, recompute `clutch_score` from the last 64 matches:
```python
state_a.clutch_score = _compute_clutch_score(state_a)
state_b.clutch_score = _compute_clutch_score(state_b)
```

**Helper functions** (in engine.py):
```python
def _specialist_score(state: PlayerState, surface: str | None) -> float | None:
    if surface is None or surface not in state.surface_elo:
        return None
    return state.surface_elo[surface] - state.elo_current

def _compute_clutch_score(state: PlayerState) -> float | None:
    records = list(state.matches)[-64:]
    if len(records) < 5:
        return None
    tb_played = sum(r.tiebreaks_played for r in records)
    tb_won = sum(r.tiebreaks_won for r in records)
    ds_played = sum(1 for r in records if r.deciding_set_played)
    ds_won = sum(1 for r in records if r.deciding_set_played and r.won)
    fsl = sum(1 for r in records if r.first_set_lost)
    fsl_won = sum(1 for r in records if r.first_set_lost and r.won)

    # Require minimum sub-samples; use neutral 0.5 default otherwise
    tb_rate = tb_won / tb_played if tb_played >= 3 else 0.5
    ds_rate = ds_won / ds_played if ds_played >= 3 else 0.5
    cb_rate = fsl_won / fsl if fsl >= 3 else 0.5

    return 0.4 * tb_rate + 0.3 * ds_rate + 0.3 * cb_rate
```

**Note on `tiebreaks_played`**: On `MatchRecord`, `tiebreaks_played` is the **match total** (both players' tiebreaks summed), while `tiebreaks_won` is per-player. This means tiebreak_win_rate = `player_tiebreaks_won / total_tiebreaks_in_match`, which is correct — a player winning 1 of 2 tiebreaks gets 0.5.

---

## 2. Score Profile Feature Group

**File**: `src/teelo/features/groups/score_profile.py` (new)
**Deletes**: `src/teelo/features/groups/dominance.py`

Absorbs all dominance features and adds new score profile + clutch matchup features.

### 2.1 Score Profile Features (two windows)

Window 8: min sample 3 (preserves existing dominance behavior). Window 64: min sample 5.

**Tiebreak win rate threshold**: Requires BOTH minimum matches in window AND non-zero `tiebreaks_played` denominator (preserving the dual-check from existing `dominance.py` lines 101–102).

| Feature | W=8 | W=64 | Sample companion |
|---|---|---|---|
| `game_diff_avg_{W}_{a,b}` | Yes | Yes | — (continuous, not a rate) |
| `set_diff_avg_{W}_{a,b}` | Yes | Yes | — (continuous, not a rate) |
| `straight_sets_rate_{W}_{a,b}` | Yes | Yes | — (denominator = matches in window) |
| `deciding_set_rate_{W}_{a,b}` | Yes | Yes | — (denominator = matches in window) |
| `tiebreak_rate_{W}_{a,b}` | Yes | Yes | — (denominator = matches in window) |
| `tiebreak_win_rate_{W}_{a,b}` | Yes | Yes | `tiebreaks_played_{W}_{a,b}` |
| `close_match_rate_{W}_{a,b}` | Yes | Yes | — (denominator = matches in window) |
| `deciding_set_win_rate_{W}_{a,b}` | — | Yes | `deciding_sets_played_64_{a,b}` |
| `comeback_rate_{W}_{a,b}` | — | Yes | `first_sets_lost_64_{a,b}` |
| `straight_sets_win_rate_{W}_{a,b}` | — | Yes | — (denominator = wins in window) |

**Note on sample size companions**: Rate features whose denominator is "matches in window" (e.g., `straight_sets_rate`) don't need explicit count companions — the denominator is always the window size (or total matches if fewer). Only features with subset denominators (tiebreaks played, deciding sets played, first sets lost) get companions, as those denominators vary meaningfully between players.

### 2.2 Clutch Matchup Features (128-match window, min sample 5)

Uses `MatchRecord.opponent_clutch_score` to bucket historical opponents:
- **Clutch**: opponent_clutch_score > 0.55
- **Normal**: 0.40 ≤ opponent_clutch_score ≤ 0.55
- **Non-clutch**: opponent_clutch_score < 0.40

Only matches where `opponent_clutch_score is not None` are considered (early-career opponents may not have enough data for a clutch score).

| Feature | Description |
|---|---|
| `vs_clutch_win_rate_{a,b}` | Win rate vs clutch opponents |
| `vs_clutch_matches_{a,b}` | Count of matches vs clutch (sample size, always populated) |
| `vs_normal_clutch_win_rate_{a,b}` | Win rate vs normal opponents |
| `vs_normal_clutch_matches_{a,b}` | Count (sample size) |
| `vs_non_clutch_win_rate_{a,b}` | Win rate vs non-clutch opponents |
| `vs_non_clutch_matches_{a,b}` | Count (sample size) |
| `opponent_clutch_score_{a,b}` | Opponent's current clutch_score (raw, not bucketed) |

### 2.3 Total Feature Count

- Window-8 features: 7 metrics × 2 players + 1 companion × 2 = **16**
- Window-64 features: 10 metrics × 2 players + 3 companions × 2 = **26**
- Clutch matchup: 7 features × 2 players = **14**
- **Total**: 56 features

### 2.4 Backward Compatibility

The W=8 features keep identical names to the old dominance group (e.g., `tiebreak_win_rate_8_a`). Existing exclusion sets in `__init__.py` (`EXCLUDED_TRIMMED_V2`, `EXCLUDED_TRIMMED_V2B`) reference these names and remain valid without changes. The group name changes from `"dominance"` to `"score_profile"` — any code keying on group name (display, neutral_display) needs updating.

---

## 3. Country Performance Feature Group

**File**: `src/teelo/features/groups/country_performance.py` (new)

All-time records (not windowed — country-specific data is already sparse). Min sample for rates: 5 matches.

| Feature | Description |
|---|---|
| `country_win_rate_{a,b}` | Win rate in tournament country |
| `country_matches_{a,b}` | Matches played in country (sample size) |
| `country_delta_{a,b}` | `country_win_rate - career_win_rate` |
| `region_win_rate_{a,b}` | Win rate in tournament region |
| `region_matches_{a,b}` | Matches played in region (sample size) |
| `region_delta_{a,b}` | `region_win_rate - career_win_rate` |
| `is_home_{a,b}` | 1.0 if player nationality == tournament country, else 0.0 |

**Total**: 14 features

**`career_win_rate` dependency**: Computed inline from `state.wins_total / (state.wins_total + state.losses_total)`. Returns `None` (and delta returns `None`) if total matches < 10, consistent with `form.py`'s threshold.

### 3.1 Region Mapping

**Location**: `src/teelo/utils/geo.py` — add `REGION_MEMBERS` dict and `ioc_to_region(ioc: str) -> str | None` function. Both `PlayerState.update()` and `country_performance.py` import from here. No circular dependencies.

```python
REGION_MEMBERS: dict[str, set[str]] = {
    "Europe": {"GBR", "FRA", "ESP", "GER", "ITA", "SUI", "AUT", "NED", "BEL", "CZE",
               "SWE", "NOR", "DEN", "FIN", "POR", "GRE", "CRO", "SRB", "ROU", "HUN",
               "POL", "BUL", "UKR", "RUS", "GEO", "LTU", "LAT", "EST", "SVK", "SLO",
               "BIH", "MNE", "MKD", "ALB", "IRL", "LUX", "AND", "MON", "CYP", "MLT",
               "ISR", ...},
    "Asia-Pacific": {"AUS", "JPN", "CHN", "KOR", "IND", "TPE", "THA", "VIE", "MAS",
                     "SGP", "INA", "PHI", "NZL", "HKG", "PAK", "SRI", "UZB", "KAZ", ...},
    "Americas": {"USA", "ARG", "BRA", "CAN", "COL", "CHI", "MEX", "PER", "ECU", "URU",
                 "VEN", "BOL", "PAR", "DOM", "PUR", "CRC", "GUA", "PAN", "JAM", ...},
    "Middle East & Africa": {"UAE", "QAT", "KSA", "RSA", "EGY", "MAR", "TUN", "NGR",
                              "KEN", "ZIM", "BRN", "OMA", "JOR", "LBN", "TUR", ...},
}

# Reverse lookup built at module load
IOC_TO_REGION: dict[str, str] = {
    ioc: region for region, members in REGION_MEMBERS.items() for ioc in members
}

def ioc_to_region(ioc: str) -> str | None:
    return IOC_TO_REGION.get(ioc)
```

Note: The `...` in sets above means the full list will be populated during implementation using all IOC codes present in the database.

---

## 4. Calendar & Surface Gap Extensions

### 4.1 Context Group Extension

**File**: `src/teelo/features/groups/context.py` (edit)

Add 3 features:

| Feature | Calculation |
|---|---|
| `month_sin` | `sin(2π × month / 12)` |
| `month_cos` | `cos(2π × month / 12)` |
| `year_progress` | `day_of_year / 365` |

All return `None` if `ctx.match_date` is None.

### 4.2 ELO Group Extension

**File**: `src/teelo/features/groups/elo.py` (edit)

Add 4 features to `EloCoreFeatures`:

| Feature | Calculation |
|---|---|
| `surface_gap_a` | `surface_elo[surface] - elo_current` for player A |
| `surface_gap_b` | Same for player B |
| `off_surface_elo_a` | Average of surface_elo values for OTHER surfaces |
| `off_surface_elo_b` | Same for player B |

Return `None` if surface is None or player has no surface ELO data.

---

## 5. Registry & Preset Changes

### `src/teelo/features/__init__.py`

- Remove `DominanceFeatures` import and registration
- Add `ScoreProfileFeatures` import and registration
- Add `CountryPerformanceFeatures` import and registration
- Existing exclusion sets (`EXCLUDED_TRIMMED_V2`, `EXCLUDED_TRIMMED_V2B`) reference W=8 feature names (e.g., `deciding_set_rate_8_a`) — these names are preserved in score_profile, so **no exclusion set changes needed**
- Add new score_profile and country_performance features to appropriate presets

### Feature Set Migration

A full backfill is required. The old `dominance` features in stored feature vectors will be replaced by the new `score_profile` features. The `INSERT ... ON CONFLICT DO UPDATE` pattern in the engine handles this — existing rows get overwritten with the full new feature set.

---

## 6. Files Changed Summary

| File | Action |
|---|---|
| `src/teelo/features/state.py` | Add fields to MatchRecord (4), PlayerState (3), MatchContext (3) |
| `src/teelo/features/engine.py` | Extend _load_matches (add country_ioc), load player nationalities, extend MatchContext construction, extend score parsing (first_set_lost), add MatchRecord fields, add clutch recompute post-update, add helper functions |
| `src/teelo/features/groups/score_profile.py` | **New** — score profile (W=8, W=64) + clutch matchup (W=128) |
| `src/teelo/features/groups/country_performance.py` | **New** — country/region win rates + is_home |
| `src/teelo/features/groups/dominance.py` | **Delete** — absorbed into score_profile |
| `src/teelo/features/groups/context.py` | Add 3 calendar features (month_sin, month_cos, year_progress) |
| `src/teelo/features/groups/elo.py` | Add 4 features to EloCoreFeatures (surface_gap, off_surface_elo) |
| `src/teelo/features/__init__.py` | Update registry (remove dominance, add score_profile + country_performance), update presets |
| `src/teelo/utils/geo.py` | Add REGION_MEMBERS, IOC_TO_REGION, ioc_to_region() |

---

## 7. Invariants & Constraints

- **No leakage**: All features computed from state BEFORE match outcome updates state. Clutch score is updated AFTER feature computation.
- **Tiebreak win rate dual-check**: Requires both minimum matches in window AND non-zero tiebreaks_played denominator (preserving existing dominance.py behavior).
- **NamedTuple ordering**: New MatchRecord fields appended after `close_match` to preserve positional args.
- **Region mapping in utility layer**: `REGION_MEMBERS` and `ioc_to_region()` live in `geo.py`, imported by both `PlayerState.update()` and `country_performance.py`.
- **Trinomial thresholds**: Clutch (0.40/0.55) are initial estimates. Calibrate from distribution after first backfill.
- **Backward compatibility**: Old `dominance` feature names preserved in score_profile W=8 — existing exclusion sets remain valid. Group name changes from "dominance" to "score_profile".
- **Country coverage**: 100% of tournaments have `country_ioc` populated (Phase 0 complete).
- **Feature count increase**: Phase 1 adds ~77 net new features (not ~36 as originally estimated) due to dual-window approach. This is intentional — feature selection will narrow later.
