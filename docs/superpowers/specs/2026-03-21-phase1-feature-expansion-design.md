# Phase 1 Feature Expansion — Design Spec

**Created**: 2026-03-21
**Status**: Draft

## Goal

Expand the feature pipeline from ~95 to ~160+ features by implementing Phase 1 of the feature expansion plan: score profile (absorbing dominance), clutch matchup, country performance, calendar context, and surface gap features.

## Design Decisions

- **Dominance merge**: The existing `dominance.py` group is absorbed into `score_profile.py`. Both 8-match and 64-match windows are kept (short = recent form, long = career tendency). `dominance.py` is deleted.
- **Sample size companions**: Every rate feature has a count companion per the plan's design principle. Rate features return `None` below minimum sample thresholds; count features always return a value.
- **Clutch classification**: Computed from 64-window score profile rates, stored on `PlayerState`, snapshotted to `MatchRecord` as `opponent_clutch_score` for historical bucketing.

---

## 1. State Changes

### 1.1 MatchRecord (NamedTuple) — 4 new fields

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

### 1.3 MatchContext (dataclass) — 3 new fields

```python
tournament_country_ioc: str | None = None   # from Tournament.country_ioc
player_a_nationality: str | None = None     # from Player.nationality_ioc
player_b_nationality: str | None = None     # from Player.nationality_ioc
```

### 1.4 Engine Changes

**`_load_matches()`**:
- Add `Tournament.country_ioc` to the SELECT columns

**Player nationality loading**:
- Load all player nationalities in a single query at startup: `{player_id: nationality_ioc}`
- Pass into `MatchContext` construction

**`MatchContext` construction**:
- Set `tournament_country_ioc` from `row.country_ioc`
- Set `player_a_nationality` and `player_b_nationality` from the nationality dict

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

**Post-update country/region record**:
In `PlayerState.update()`, if `record.country_ioc` is not None:
- Update `self.country_record[record.country_ioc]`
- Resolve region from IOC code, update `self.region_record[region]`

**Helper functions** (in engine.py or a shared utility):
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

    tb_rate = tb_won / tb_played if tb_played >= 3 else 0.5  # neutral default
    ds_rate = ds_won / ds_played if ds_played >= 3 else 0.5
    cb_rate = fsl_won / fsl if fsl >= 3 else 0.5

    return 0.4 * tb_rate + 0.3 * ds_rate + 0.3 * cb_rate
```

---

## 2. Score Profile Feature Group

**File**: `src/teelo/features/groups/score_profile.py` (new)
**Deletes**: `src/teelo/features/groups/dominance.py`

Absorbs all dominance features and adds new score profile + clutch matchup features.

### 2.1 Score Profile Features (two windows)

Window 8: min sample 3. Window 64: min sample 5.

| Feature | W=8 | W=64 | Sample companion |
|---|---|---|---|
| `game_diff_avg_{W}_{a,b}` | Yes | Yes | — |
| `set_diff_avg_{W}_{a,b}` | Yes | Yes | — |
| `straight_sets_rate_{W}_{a,b}` | Yes | Yes | — |
| `deciding_set_rate_{W}_{a,b}` | Yes | Yes | — |
| `tiebreak_rate_{W}_{a,b}` | Yes | Yes | — |
| `tiebreak_win_rate_{W}_{a,b}` | Yes | Yes | `tiebreaks_played_{W}_{a,b}` |
| `close_match_rate_{W}_{a,b}` | Yes | Yes | — |
| `deciding_set_win_rate_{W}_{a,b}` | — | Yes | `deciding_sets_played_64_{a,b}` |
| `comeback_rate_{W}_{a,b}` | — | Yes | `first_sets_lost_64_{a,b}` |
| `straight_sets_win_rate_{W}_{a,b}` | — | Yes | — |

### 2.2 Clutch Matchup Features (128-match window, min sample 5)

Uses `MatchRecord.opponent_clutch_score` to bucket historical opponents:
- **Clutch**: opponent_clutch_score > 0.55
- **Normal**: 0.40 ≤ opponent_clutch_score ≤ 0.55
- **Non-clutch**: opponent_clutch_score < 0.40

| Feature | Description |
|---|---|
| `vs_clutch_win_rate_{a,b}` | Win rate vs clutch opponents |
| `vs_clutch_matches_{a,b}` | Count of matches vs clutch (sample size) |
| `vs_normal_clutch_win_rate_{a,b}` | Win rate vs normal opponents |
| `vs_normal_clutch_matches_{a,b}` | Count (sample size) |
| `vs_non_clutch_win_rate_{a,b}` | Win rate vs non-clutch opponents |
| `vs_non_clutch_matches_{a,b}` | Count (sample size) |
| `opponent_clutch_score_{a,b}` | Opponent's current clutch_score (raw, not bucketed) |

### 2.3 Total Feature Count

- Window-8 features: 7 rates × 2 players + 1 companion × 2 = **16**
- Window-64 features: 10 rates × 2 players + 3 companions × 2 = **26**
- Clutch matchup: 7 features × 2 players = **14**
- **Total**: 56 features

---

## 3. Country Performance Feature Group

**File**: `src/teelo/features/groups/country_performance.py` (new)

All-time records (not windowed). Min sample for rates: 5 matches.

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

### Region Mapping

IOC code → region lookup. Module-level constant:

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
```

Build a reverse lookup `IOC_TO_REGION: dict[str, str]` at module load.

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
- Update all presets that reference dominance feature names to use new score_profile names

### Feature Set Migration

A full backfill is required. The old `dominance` features in stored feature vectors will be replaced by the new `score_profile` features. The `INSERT ... ON CONFLICT DO UPDATE` pattern in the engine handles this — existing rows get overwritten.

---

## 6. Files Changed Summary

| File | Action |
|---|---|
| `src/teelo/features/state.py` | Add fields to MatchRecord, PlayerState, MatchContext |
| `src/teelo/features/engine.py` | Extend _load_matches, MatchContext construction, score parsing, state update, add helpers |
| `src/teelo/features/groups/score_profile.py` | **New** — score profile + clutch matchup |
| `src/teelo/features/groups/country_performance.py` | **New** — country/region performance |
| `src/teelo/features/groups/dominance.py` | **Delete** — absorbed into score_profile |
| `src/teelo/features/groups/context.py` | Add calendar features |
| `src/teelo/features/groups/elo.py` | Add surface gap features |
| `src/teelo/features/__init__.py` | Update registry, presets |
| `src/teelo/utils/geo.py` | Add region mapping if not already present |

---

## 7. Invariants & Constraints

- **No leakage**: All features computed from state BEFORE match outcome updates state. Clutch score is updated AFTER feature computation.
- **Sample size companions**: Every rate feature has a count companion. Rates return `None` below threshold; counts always return a value (even 0).
- **Trinomial thresholds**: Clutch (0.40/0.55) are initial estimates. Calibrate from distribution after first backfill.
- **Backward compatibility**: Old `dominance` feature names disappear. Any code referencing them (presets, display) must be updated.
- **Country coverage**: 100% of tournaments have `country_ioc` populated (Phase 0 complete).
