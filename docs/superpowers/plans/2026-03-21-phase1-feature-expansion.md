# Phase 1 Feature Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the ML feature pipeline from ~158 to ~235 features by adding score profile (absorbing dominance), clutch matchup, country performance, calendar context, and surface gap features.

**Architecture:** Modular feature groups registered via `FeatureRegistry`. Each group subclasses `FeatureGroup` ABC, implements `compute()` returning `dict[str, float | None]`. State accumulates in `PlayerState` during chronological replay in `engine.py`. Features computed BEFORE state update (no leakage).

**Tech Stack:** Python 3.12, SQLAlchemy 2.0, PostgreSQL, pytest

**Spec:** `docs/superpowers/specs/2026-03-21-phase1-feature-expansion-design.md`

---

### Task 1: Add new fields to MatchRecord

**Files:**
- Modify: `src/teelo/features/state.py:31` (append after `close_match`)
- Modify: `tests/unit/test_feature_groups_v2.py:6-50` (update `_record` helper)

- [ ] **Step 1: Write test for new MatchRecord fields**

Create `tests/unit/test_feature_state.py`:

```python
def test_match_record_new_fields_default() -> None:
    """New Phase 1 fields should have sensible defaults."""
    r = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
    )
    assert r.first_set_lost is False
    assert r.opponent_clutch_score is None
    assert r.opponent_specialist_score is None
    assert r.country_ioc is None


def test_match_record_new_fields_explicit() -> None:
    r = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        first_set_lost=True, opponent_clutch_score=0.52,
        opponent_specialist_score=45.0, country_ioc="GBR",
    )
    assert r.first_set_lost is True
    assert r.opponent_clutch_score == 0.52
    assert r.opponent_specialist_score == 45.0
    assert r.country_ioc == "GBR"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && pytest tests/unit/test_feature_state.py::test_match_record_new_fields_default -v`
Expected: FAIL — `TypeError: MatchRecord.__new__() got an unexpected keyword argument 'first_set_lost'`

- [ ] **Step 3: Add fields to MatchRecord**

In `src/teelo/features/state.py`, append after line 31 (`close_match: bool = False`):

```python
    first_set_lost: bool = False
    opponent_clutch_score: float | None = None
    opponent_specialist_score: float | None = None
    country_ioc: str | None = None
```

- [ ] **Step 4: Update test helper `_record` in test_feature_groups_v2.py**

Add new keyword args to the `_record()` function signature (after `close_match` param at line 23):

```python
    first_set_lost: bool = False,
    opponent_clutch_score: float | None = None,
    opponent_specialist_score: float | None = None,
    country_ioc: str | None = None,
```

And pass them through in the `MatchRecord(...)` call inside `_record()`:

```python
        first_set_lost=first_set_lost,
        opponent_clutch_score=opponent_clutch_score,
        opponent_specialist_score=opponent_specialist_score,
        country_ioc=country_ioc,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/unit/test_feature_state.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/teelo/features/state.py tests/unit/test_feature_state.py tests/unit/test_feature_groups_v2.py
git commit -m "feat: add Phase 1 fields to MatchRecord (first_set_lost, clutch/specialist scores, country_ioc)"
```

---

### Task 2: Add new fields to PlayerState and MatchContext

**Files:**
- Modify: `src/teelo/features/state.py:57` (MatchContext), `src/teelo/features/state.py:80` (PlayerState)

- [ ] **Step 1: Write tests for new fields**

Add to `tests/unit/test_feature_state.py`:

```python
def test_match_context_new_fields() -> None:
    ctx = MatchContext(
        match_id=1, match_date=date(2026, 1, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        tournament_country_ioc="GBR",
        player_a_nationality="ESP",
        player_b_nationality="GBR",
    )
    assert ctx.tournament_country_ioc == "GBR"
    assert ctx.player_a_nationality == "ESP"
    assert ctx.player_b_nationality == "GBR"


def test_match_context_new_fields_default() -> None:
    ctx = MatchContext(
        match_id=1, match_date=date(2026, 1, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1,
    )
    assert ctx.tournament_country_ioc is None
    assert ctx.player_a_nationality is None
    assert ctx.player_b_nationality is None


def test_player_state_new_fields() -> None:
    state = PlayerState(player_id=1)
    assert state.clutch_score is None
    assert state.country_record == {}
    assert state.region_record == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_feature_state.py::test_match_context_new_fields -v`
Expected: FAIL — `TypeError: MatchContext.__init__() got an unexpected keyword argument`

- [ ] **Step 3: Add fields to MatchContext**

In `src/teelo/features/state.py`, append after line 57 (`match_date_estimated: bool = False`):

```python
    tournament_country_ioc: str | None = None
    player_a_nationality: str | None = None
    player_b_nationality: str | None = None
```

- [ ] **Step 4: Add fields to PlayerState**

In `src/teelo/features/state.py`, append after line 80 (`tournament_losses`):

```python
    clutch_score: float | None = None
    country_record: dict[str, tuple[int, int]] = field(default_factory=dict)
    region_record: dict[str, tuple[int, int]] = field(default_factory=dict)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/unit/test_feature_state.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/teelo/features/state.py tests/unit/test_feature_state.py
git commit -m "feat: add Phase 1 fields to MatchContext and PlayerState"
```

---

### Task 3: Add region mapping to geo.py

**Files:**
- Modify: `src/teelo/utils/geo.py` (append region mapping after existing code)
- Create: `tests/unit/test_geo_regions.py`

- [ ] **Step 1: Write tests for region mapping**

Create `tests/unit/test_geo_regions.py`:

```python
from teelo.utils.geo import ioc_to_region, REGION_MEMBERS, IOC_TO_REGION


def test_ioc_to_region_known_codes() -> None:
    assert ioc_to_region("GBR") == "Europe"
    assert ioc_to_region("USA") == "Americas"
    assert ioc_to_region("AUS") == "Asia-Pacific"
    assert ioc_to_region("UAE") == "Middle East & Africa"


def test_ioc_to_region_unknown() -> None:
    assert ioc_to_region("XYZ") is None


def test_no_duplicate_ioc_across_regions() -> None:
    all_codes: list[str] = []
    for members in REGION_MEMBERS.values():
        all_codes.extend(members)
    assert len(all_codes) == len(set(all_codes)), "Duplicate IOC code across regions"


def test_region_members_covers_major_tours() -> None:
    # Spot check: all Grand Slam host countries are mapped
    for ioc in ["AUS", "FRA", "GBR", "USA"]:
        assert ioc_to_region(ioc) is not None, f"{ioc} missing from region mapping"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_geo_regions.py -v`
Expected: FAIL — `ImportError: cannot import name 'ioc_to_region'`

- [ ] **Step 3: Add region mapping to geo.py**

Append to the end of `src/teelo/utils/geo.py`:

```python
# ---------------------------------------------------------------------------
# Region mapping (IOC code → region)
# ---------------------------------------------------------------------------

REGION_MEMBERS: dict[str, set[str]] = {
    "Europe": {
        "ALB", "AND", "ARM", "AUT", "AZE", "BEL", "BIH", "BLR", "BUL",
        "CRO", "CYP", "CZE", "DEN", "ESP", "EST", "FIN", "FRA", "GBR",
        "GEO", "GER", "GRE", "HUN", "IRL", "ISL", "ISR", "ITA", "LAT",
        "LTU", "LUX", "MDA", "MKD", "MLT", "MNE", "MON", "NED", "NOR",
        "POL", "POR", "ROU", "RUS", "SLO", "SRB", "SVK", "SWE", "SUI",
        "UKR",
    },
    "Asia-Pacific": {
        "AUS", "BAN", "CHN", "HKG", "INA", "IND", "JPN", "KAZ", "KGZ",
        "KOR", "MAS", "MYA", "NZL", "PAK", "PHI", "SGP", "SRI", "THA",
        "TJK", "TKM", "TPE", "UZB", "VIE",
    },
    "Americas": {
        "ARG", "BAH", "BAR", "BER", "BOL", "BRA", "CAN", "CHI", "COL",
        "CRC", "CUB", "DOM", "ECU", "ESA", "GUA", "HAI", "HON", "JAM",
        "MEX", "NCA", "PAN", "PAR", "PER", "PUR", "TTO", "URU", "USA",
        "VEN",
    },
    "Middle East & Africa": {
        "ALG", "ANG", "BDI", "BEN", "BOT", "BRN", "BUR", "CAM", "CGO",
        "CHA", "CIV", "CMR", "COD", "COM", "DJI", "EGY", "ERI", "ETH",
        "GAB", "GAM", "GHA", "GUI", "GNB", "IRQ", "JOR", "KEN", "KSA",
        "KUW", "LBA", "LBN", "LBR", "LES", "MAD", "MAR", "MAW", "MLI",
        "MOZ", "MRI", "MTN", "NAM", "NGR", "NIG", "OMA", "QAT", "RSA",
        "RWA", "SEN", "SEY", "SLE", "SOM", "SSD", "SUD", "SWZ", "SYR",
        "TAN", "TOG", "TUN", "TUR", "UAE", "UGA", "YEM", "ZAM", "ZIM",
    },
}

IOC_TO_REGION: dict[str, str] = {
    ioc: region for region, members in REGION_MEMBERS.items() for ioc in members
}


def ioc_to_region(ioc: str) -> str | None:
    """Return the region name for an IOC country code, or None if unknown."""
    return IOC_TO_REGION.get(ioc)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_geo_regions.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/utils/geo.py tests/unit/test_geo_regions.py
git commit -m "feat: add IOC-to-region mapping in geo.py for country performance features"
```

---

### Task 4: Update PlayerState.update() for country/region tracking

**Files:**
- Modify: `src/teelo/features/state.py:82-127` (update method)

- [ ] **Step 1: Write test for country/region tracking**

Add to `tests/unit/test_feature_state.py`:

```python
def test_player_state_update_tracks_country_record() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc="GBR",
    )
    state.update(record, 1510.0, 1510.0)
    assert state.country_record["GBR"] == (1, 0)

    record2 = MatchRecord(
        temporal_order=2, won=False, surface="Hard", level_code="A",
        games_won=3, games_lost=6, tournament_edition_id=2,
        tournament_id=1, match_date=date(2026, 2, 1), opponent_id=3,
        country_ioc="GBR",
    )
    state.update(record2, 1505.0, 1505.0)
    assert state.country_record["GBR"] == (1, 1)


def test_player_state_update_tracks_region_record() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc="GBR",
    )
    state.update(record, 1510.0, 1510.0)
    assert state.region_record["Europe"] == (1, 0)


def test_player_state_update_skips_country_when_none() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc=None,
    )
    state.update(record, 1510.0, 1510.0)
    assert state.country_record == {}
    assert state.region_record == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_feature_state.py::test_player_state_update_tracks_country_record -v`
Expected: FAIL — `KeyError: 'GBR'` (country_record not populated)

- [ ] **Step 3: Add country/region tracking to PlayerState.update()**

In `src/teelo/features/state.py`, add import at top:

```python
from teelo.utils.geo import ioc_to_region
```

Then in `PlayerState.update()`, append after the h2h block (after line 126, before the closing of the method):

```python
        if record.country_ioc is not None:
            wins, losses = self.country_record.get(record.country_ioc, (0, 0))
            if record.won:
                self.country_record[record.country_ioc] = (wins + 1, losses)
            else:
                self.country_record[record.country_ioc] = (wins, losses + 1)
            region = ioc_to_region(record.country_ioc)
            if region is not None:
                rwins, rlosses = self.region_record.get(region, (0, 0))
                if record.won:
                    self.region_record[region] = (rwins + 1, rlosses)
                else:
                    self.region_record[region] = (rwins, rlosses + 1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_feature_state.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/state.py tests/unit/test_feature_state.py
git commit -m "feat: track country/region win-loss records in PlayerState.update()"
```

---

### Task 5: Update engine.py — load country_ioc and player nationalities

**Files:**
- Modify: `src/teelo/features/engine.py:286-319` (_load_matches), `src/teelo/features/engine.py:46-122` (run method)

- [ ] **Step 1: Add Tournament.country_ioc to _load_matches() SELECT**

In `src/teelo/features/engine.py`, in the `_load_matches()` method, add after `Tournament.level` (line 312):

```python
                Tournament.country_ioc.label("tournament_country_ioc"),
```

- [ ] **Step 2: Add Player nationality loading**

At the top of `engine.py`, add import:

```python
from teelo.db.models import Player
```

In the `run()` method, after the `_load_matches()` call (after line 58), add:

```python
            # Load player nationalities for country performance features
            nationality_rows = session.execute(
                select(Player.id, Player.nationality_ioc)
            ).all()
            nationalities: dict[int, str | None] = {
                row.id: row.nationality_ioc for row in nationality_rows
            }
```

- [ ] **Step 3: Update MatchContext construction**

In `engine.py`, extend the MatchContext construction (after line 121, `match_date_estimated`):

```python
                    tournament_country_ioc=row.tournament_country_ioc,
                    player_a_nationality=nationalities.get(row.player_a_id),
                    player_b_nationality=nationalities.get(row.player_b_id),
```

- [ ] **Step 4: Run existing tests to check nothing broke**

Run: `pytest tests/ -v --timeout=30`
Expected: All existing tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/engine.py
git commit -m "feat: load tournament country_ioc and player nationalities in engine"
```

---

### Task 6: Update engine.py — first_set_lost parsing and new MatchRecord fields

**Files:**
- Modify: `src/teelo/features/engine.py:142-222` (score parsing + record construction)

- [ ] **Step 1: Write test for first_set_lost parsing**

Create `tests/unit/test_engine_score_parsing.py`:

```python
from teelo.features.engine import _compute_score_summary


def test_compute_score_summary_returns_four_tuple() -> None:
    score = [{"a": 6, "b": 4}, {"a": 3, "b": 6}, {"a": 7, "b": 5}]
    result = _compute_score_summary(score)
    assert result == (2, 1, 0, 0)


def test_compute_score_summary_with_tiebreak() -> None:
    score = [
        {"a": 7, "b": 6, "tb_a": 7, "tb_b": 3},
        {"a": 6, "b": 4},
    ]
    result = _compute_score_summary(score)
    assert result == (2, 0, 1, 0)
```

- [ ] **Step 2: Run test to verify it passes (existing function)**

Run: `pytest tests/unit/test_engine_score_parsing.py -v`
Expected: PASS (testing existing function)

- [ ] **Step 3: Add first_set_lost computation and new MatchRecord fields in engine.py**

In `engine.py`, after the `close_match` computation (after line 174), add:

```python
                    # Compute first_set_lost for comeback tracking
                    first_set_lost_a = False
                    first_set_lost_b = False
                    if row.score_structured and len(row.score_structured) > 0:
                        first_set = row.score_structured[0]
                        fs_a = first_set.get("a", 0)
                        fs_b = first_set.get("b", 0)
                        first_set_lost_a = fs_a < fs_b
                        first_set_lost_b = fs_b < fs_a
```

Then in `record_a` construction (after line 196, `close_match=close_match`), add:

```python
                        first_set_lost=first_set_lost_a,
                        opponent_clutch_score=state_b.clutch_score,
                        opponent_specialist_score=_specialist_score(state_b, surface),
                        country_ioc=ctx.tournament_country_ioc,
```

And in `record_b` construction (after line 218, `close_match=close_match`), add:

```python
                        first_set_lost=first_set_lost_b,
                        opponent_clutch_score=state_a.clutch_score,
                        opponent_specialist_score=_specialist_score(state_a, surface),
                        country_ioc=ctx.tournament_country_ioc,
```

- [ ] **Step 4: Add helper functions to engine.py**

Add before the `_compute_games()` function (module-level):

```python
def _specialist_score(state: PlayerState, surface: str | None) -> float | None:
    """Compute surface specialist score: surface_elo - overall_elo."""
    if surface is None or surface not in state.surface_elo:
        return None
    return state.surface_elo[surface] - state.elo_current


def _compute_clutch_score(state: PlayerState) -> float | None:
    """Compute rolling clutch score from last 64 matches."""
    records = list(state.matches)[-64:]
    if len(records) < 5:
        return None
    tb_played = sum(r.tiebreaks_played for r in records)
    tb_won = sum(r.tiebreaks_won for r in records)
    ds_played = sum(1 for r in records if r.deciding_set_played)
    ds_won = sum(1 for r in records if r.deciding_set_played and r.won)
    fsl = sum(1 for r in records if r.first_set_lost)
    fsl_won = sum(1 for r in records if r.first_set_lost and r.won)

    tb_rate = tb_won / tb_played if tb_played >= 3 else 0.5
    ds_rate = ds_won / ds_played if ds_played >= 3 else 0.5
    cb_rate = fsl_won / fsl if fsl >= 3 else 0.5

    return 0.4 * tb_rate + 0.3 * ds_rate + 0.3 * cb_rate
```

- [ ] **Step 5: Add clutch score recomputation after state updates**

In `engine.py`, after the state update calls (after line 222 `state_b.update(...)` and `updated_states += 1`), add:

```python
                    state_a.clutch_score = _compute_clutch_score(state_a)
                    state_b.clutch_score = _compute_clutch_score(state_b)
```

- [ ] **Step 6: Write tests for helper functions**

Add to `tests/unit/test_engine_score_parsing.py`:

```python
from datetime import date
from teelo.features.engine import _compute_clutch_score, _specialist_score
from teelo.features.state import MatchRecord, PlayerState


def test_specialist_score_no_surface() -> None:
    state = PlayerState(player_id=1)
    assert _specialist_score(state, None) is None


def test_specialist_score_with_surface() -> None:
    state = PlayerState(player_id=1, elo_current=1600.0)
    state.surface_elo["Clay"] = 1650.0
    assert _specialist_score(state, "Clay") == 50.0


def test_clutch_score_insufficient_matches() -> None:
    state = PlayerState(player_id=1)
    assert _compute_clutch_score(state) is None


def test_clutch_score_neutral_when_no_sub_events() -> None:
    """With 10 matches but no tiebreaks/deciding sets/first sets lost, defaults to 0.5."""
    state = PlayerState(player_id=1)
    for i in range(10):
        state.matches.append(MatchRecord(
            temporal_order=i, won=True, surface="Hard", level_code="A",
            games_won=6, games_lost=3, tournament_edition_id=1,
            tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        ))
    score = _compute_clutch_score(state)
    assert score is not None
    assert abs(score - 0.5) < 0.01  # neutral default
```

- [ ] **Step 7: Run all tests**

Run: `pytest tests/ -v --timeout=30`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/teelo/features/engine.py tests/unit/test_engine_score_parsing.py
git commit -m "feat: add first_set_lost parsing, clutch/specialist helpers, and new MatchRecord fields in engine"
```

---

### Task 7: Implement ScoreProfileFeatures group

**Files:**
- Create: `src/teelo/features/groups/score_profile.py`
- Create: `tests/unit/test_score_profile.py`

- [ ] **Step 1: Write tests for score profile features**

Create `tests/unit/test_score_profile.py`:

```python
from datetime import date

from teelo.features.groups.score_profile import ScoreProfileFeatures
from teelo.features.state import MatchContext, MatchRecord, PlayerState


def _record(
    *,
    temporal_order: int,
    won: bool,
    games_won: int = 6,
    games_lost: int = 3,
    sets_won: int = 2,
    sets_lost: int = 0,
    tiebreaks_played: int = 0,
    tiebreaks_won: int = 0,
    deciding_set_played: bool = False,
    straight_sets: bool = False,
    close_match: bool = False,
    first_set_lost: bool = False,
    opponent_clutch_score: float | None = None,
) -> MatchRecord:
    return MatchRecord(
        temporal_order=temporal_order, won=won, surface="Hard",
        level_code="A", games_won=games_won, games_lost=games_lost,
        tournament_edition_id=1, tournament_id=1,
        match_date=date(2026, 1, 1), opponent_id=2,
        sets_won=sets_won, sets_lost=sets_lost,
        tiebreaks_played=tiebreaks_played, tiebreaks_won=tiebreaks_won,
        deciding_set_played=deciding_set_played,
        straight_sets=straight_sets, close_match=close_match,
        first_set_lost=first_set_lost,
        opponent_clutch_score=opponent_clutch_score,
    )


def _ctx() -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_returns_default_with_no_matches() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    # No matches — rates return 0.5 (uninformative), diffs return 0.0
    assert features["straight_sets_rate_8_a"] == 0.5
    assert features["tiebreak_win_rate_8_a"] == 0.5
    assert features["game_diff_avg_8_a"] == 0.0
    assert features["tiebreaks_played_8_a"] == 0


def test_window_8_straight_sets_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(4):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            straight_sets=(i < 2),  # 2 of 4 are straight sets
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["straight_sets_rate_8_a"] == 0.5


def test_tiebreak_win_rate_default_when_no_tiebreaks() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(5):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            tiebreaks_played=0, tiebreaks_won=0,
        ))
    features = group.compute(state_a, state_b, _ctx())
    # No tiebreaks played — returns 0.5 default, not None
    assert features["tiebreak_win_rate_8_a"] == 0.5
    assert features["tiebreaks_played_8_a"] == 0


def test_window_64_deciding_set_win_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=(i < 7),  # 7 wins, 3 losses
            deciding_set_played=(i < 6),  # 6 deciding sets
            sets_won=2, sets_lost=1,
        ))
    features = group.compute(state_a, state_b, _ctx())
    # 6 deciding sets, first 6 matches; 6 wins out of 7 in deciding sets but only first 6 have deciding sets
    # Matches 0-5: deciding_set_played=True, won=True (6 wins in deciding)
    # Match 6: deciding_set_played=False, won=True
    # Matches 7-9: deciding_set_played=False, won=False
    assert features["deciding_sets_played_64_a"] == 6
    assert features["deciding_set_win_rate_64_a"] == 1.0  # 6/6


def test_comeback_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=(i < 5),
            first_set_lost=(i < 8),  # 8 lost first set
        ))
    features = group.compute(state_a, state_b, _ctx())
    # 8 first sets lost, 5 of those are wins
    assert features["first_sets_lost_64_a"] == 8
    assert features["comeback_rate_64_a"] == 5 / 8


def test_clutch_matchup_features() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    # Give player A matches against clutch (>0.55) and non-clutch (<0.40)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            opponent_clutch_score=0.60 if i < 5 else 0.30,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["vs_clutch_matches_a"] == 5
    assert features["vs_clutch_win_rate_a"] == 1.0
    assert features["vs_non_clutch_matches_a"] == 5
    assert features["vs_non_clutch_win_rate_a"] == 1.0


def test_clutch_matchup_all_none_opponents() -> None:
    """When all opponents lack clutch scores, matchup rates default to 0.5, counts 0."""
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            opponent_clutch_score=None,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["vs_clutch_win_rate_a"] == 0.5
    assert features["vs_clutch_matches_a"] == 0
    assert features["vs_normal_clutch_matches_a"] == 0
    assert features["vs_non_clutch_matches_a"] == 0


def test_feature_names_count() -> None:
    group = ScoreProfileFeatures()
    names = group.feature_names()
    assert len(names) == 56
    assert group.name == "score_profile"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_score_profile.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'teelo.features.groups.score_profile'`

- [ ] **Step 3: Implement ScoreProfileFeatures**

Create `src/teelo/features/groups/score_profile.py`:

```python
"""Score profile, clutch classification, and clutch matchup features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_W8 = 8
_W64 = 64
_W128 = 128

# Clutch classification thresholds
_CLUTCH_HIGH = 0.55
_CLUTCH_LOW = 0.40


def _records(state: PlayerState, limit: int) -> list[MatchRecord]:
    return list(state.matches)[-limit:]


def _rate(numerator: float, denominator: int) -> float:
    """Return rate, or 0.5 (uninformative) when denominator is 0."""
    if denominator == 0:
        return 0.5
    return numerator / denominator


def _average(values: list[float]) -> float:
    """Return mean, or 0.0 (neutral) when empty."""
    if not values:
        return 0.0
    return sum(values) / len(values)


# ---- Window-based score profile features ----

def _game_diff_avg(records: list[MatchRecord]) -> float:
    return _average([float(r.games_won - r.games_lost) for r in records])


def _set_diff_avg(records: list[MatchRecord]) -> float:
    return _average([float(r.sets_won - r.sets_lost) for r in records])


def _straight_sets_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.straight_sets), len(records))


def _deciding_set_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.deciding_set_played), len(records))


def _tiebreak_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.tiebreaks_played > 0), len(records))


def _tiebreak_win_rate(records: list[MatchRecord]) -> float:
    denom = sum(r.tiebreaks_played for r in records)
    if denom == 0:
        return 0.5
    return sum(r.tiebreaks_won for r in records) / denom


def _tiebreaks_played_count(records: list[MatchRecord]) -> float:
    return float(sum(r.tiebreaks_played for r in records))


def _close_match_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.close_match), len(records))


def _deciding_set_win_rate(records: list[MatchRecord]) -> float:
    ds = [r for r in records if r.deciding_set_played]
    if not ds:
        return 0.5
    return sum(1.0 for r in ds if r.won) / len(ds)


def _deciding_sets_played(records: list[MatchRecord]) -> float:
    return float(sum(1 for r in records if r.deciding_set_played))


def _comeback_rate(records: list[MatchRecord]) -> float:
    fsl = [r for r in records if r.first_set_lost]
    if not fsl:
        return 0.5
    return sum(1.0 for r in fsl if r.won) / len(fsl)


def _first_sets_lost(records: list[MatchRecord]) -> float:
    return float(sum(1 for r in records if r.first_set_lost))


def _straight_sets_win_rate(records: list[MatchRecord]) -> float:
    wins = [r for r in records if r.won]
    if not wins:
        return 0.5
    return sum(1.0 for r in wins if r.straight_sets) / len(wins)


# ---- Clutch matchup features ----

def _clutch_bucket_stats(
    records: list[MatchRecord],
    low: float,
    high: float,
) -> dict[str, tuple[int, int, int]]:
    """Bucket records by opponent clutch score.

    Returns dict with keys "clutch", "normal", "non_clutch", each mapping
    to (wins, losses, total).
    """
    buckets: dict[str, tuple[int, int, int]] = {
        "clutch": (0, 0, 0),
        "normal": (0, 0, 0),
        "non_clutch": (0, 0, 0),
    }
    for r in records:
        if r.opponent_clutch_score is None:
            continue
        if r.opponent_clutch_score > high:
            key = "clutch"
        elif r.opponent_clutch_score < low:
            key = "non_clutch"
        else:
            key = "normal"
        w, l, t = buckets[key]
        if r.won:
            buckets[key] = (w + 1, l, t + 1)
        else:
            buckets[key] = (w, l + 1, t + 1)
    return buckets


class ScoreProfileFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "score_profile"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        # Window 8 features (14 per-player features + 2 companions = 16)
        for suffix in ("a", "b"):
            for feat in (
                "game_diff_avg_8",
                "set_diff_avg_8",
                "straight_sets_rate_8",
                "deciding_set_rate_8",
                "tiebreak_rate_8",
                "tiebreak_win_rate_8",
                "tiebreaks_played_8",
                "close_match_rate_8",
            ):
                names.append(f"{feat}_{suffix}")
        # Window 64 features (20 per-player features + 6 companions = 26)
        for suffix in ("a", "b"):
            for feat in (
                "game_diff_avg_64",
                "set_diff_avg_64",
                "straight_sets_rate_64",
                "deciding_set_rate_64",
                "tiebreak_rate_64",
                "tiebreak_win_rate_64",
                "tiebreaks_played_64",
                "close_match_rate_64",
                "deciding_set_win_rate_64",
                "deciding_sets_played_64",
                "comeback_rate_64",
                "first_sets_lost_64",
                "straight_sets_win_rate_64",
            ):
                names.append(f"{feat}_{suffix}")
        # Clutch matchup features (14)
        for suffix in ("a", "b"):
            for feat in (
                "vs_clutch_win_rate",
                "vs_clutch_matches",
                "vs_normal_clutch_win_rate",
                "vs_normal_clutch_matches",
                "vs_non_clutch_win_rate",
                "vs_non_clutch_matches",
                "opponent_clutch_score",
            ):
                names.append(f"{feat}_{suffix}")
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        for suffix, state in (("a", state_a), ("b", state_b)):
            r8 = _records(state, _W8)
            r64 = _records(state, _W64)
            r128 = _records(state, _W128)

            # Window 8
            features[f"game_diff_avg_8_{suffix}"] = _game_diff_avg(r8)
            features[f"set_diff_avg_8_{suffix}"] = _set_diff_avg(r8)
            features[f"straight_sets_rate_8_{suffix}"] = _straight_sets_rate(r8)
            features[f"deciding_set_rate_8_{suffix}"] = _deciding_set_rate(r8)
            features[f"tiebreak_rate_8_{suffix}"] = _tiebreak_rate(r8)
            features[f"tiebreak_win_rate_8_{suffix}"] = _tiebreak_win_rate(r8)
            features[f"tiebreaks_played_8_{suffix}"] = _tiebreaks_played_count(r8)
            features[f"close_match_rate_8_{suffix}"] = _close_match_rate(r8)

            # Window 64
            features[f"game_diff_avg_64_{suffix}"] = _game_diff_avg(r64)
            features[f"set_diff_avg_64_{suffix}"] = _set_diff_avg(r64)
            features[f"straight_sets_rate_64_{suffix}"] = _straight_sets_rate(r64)
            features[f"deciding_set_rate_64_{suffix}"] = _deciding_set_rate(r64)
            features[f"tiebreak_rate_64_{suffix}"] = _tiebreak_rate(r64)
            features[f"tiebreak_win_rate_64_{suffix}"] = _tiebreak_win_rate(r64)
            features[f"tiebreaks_played_64_{suffix}"] = _tiebreaks_played_count(r64)
            features[f"close_match_rate_64_{suffix}"] = _close_match_rate(r64)
            features[f"deciding_set_win_rate_64_{suffix}"] = _deciding_set_win_rate(r64)
            features[f"deciding_sets_played_64_{suffix}"] = _deciding_sets_played(r64)
            features[f"comeback_rate_64_{suffix}"] = _comeback_rate(r64)
            features[f"first_sets_lost_64_{suffix}"] = _first_sets_lost(r64)
            features[f"straight_sets_win_rate_64_{suffix}"] = _straight_sets_win_rate(r64)

            # Clutch matchup
            buckets = _clutch_bucket_stats(r128, _CLUTCH_LOW, _CLUTCH_HIGH)
            for bucket_key, feat_prefix in (
                ("clutch", "vs_clutch"),
                ("normal", "vs_normal_clutch"),
                ("non_clutch", "vs_non_clutch"),
            ):
                w, _l, t = buckets[bucket_key]
                features[f"{feat_prefix}_matches_{suffix}"] = float(t)
                features[f"{feat_prefix}_win_rate_{suffix}"] = (
                    w / t if t > 0 else 0.5
                )

            # Opponent's current clutch score
            opp = state_b if suffix == "a" else state_a
            features[f"opponent_clutch_score_{suffix}"] = opp.clutch_score

        return features
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_score_profile.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/groups/score_profile.py tests/unit/test_score_profile.py
git commit -m "feat: implement ScoreProfileFeatures group (absorbs dominance, adds W64 + clutch matchup)"
```

---

### Task 8: Implement CountryPerformanceFeatures group

**Files:**
- Create: `src/teelo/features/groups/country_performance.py`
- Create: `tests/unit/test_country_performance.py`

- [ ] **Step 1: Write tests**

Create `tests/unit/test_country_performance.py`:

```python
from datetime import date

from teelo.features.groups.country_performance import CountryPerformanceFeatures
from teelo.features.state import MatchContext, MatchRecord, PlayerState


def _state_with_country_record(
    wins_total: int = 20,
    losses_total: int = 10,
    country_record: dict | None = None,
    region_record: dict | None = None,
    nationality: str | None = None,
) -> PlayerState:
    state = PlayerState(player_id=1)
    state.wins_total = wins_total
    state.losses_total = losses_total
    if country_record:
        state.country_record = country_record
    if region_record:
        state.region_record = region_record
    return state


def _ctx(
    country_ioc: str | None = "GBR",
    player_a_nationality: str | None = "ESP",
    player_b_nationality: str | None = "GBR",
) -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
        tournament_country_ioc=country_ioc,
        player_a_nationality=player_a_nationality,
        player_b_nationality=player_b_nationality,
    )


def test_country_win_rate() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        country_record={"GBR": (4, 1)},
        region_record={"Europe": (10, 5)},
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    assert features["country_win_rate_a"] == 4 / 5
    assert features["country_matches_a"] == 5


def test_country_win_rate_small_sample() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        country_record={"GBR": (2, 1)},  # only 3 matches — still computed
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    assert features["country_win_rate_a"] == 2 / 3
    assert features["country_matches_a"] == 3


def test_country_delta() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        wins_total=20, losses_total=10,  # career 66.7%
        country_record={"GBR": (4, 1)},  # country 80%
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    expected_delta = (4 / 5) - (20 / 30)
    assert features["country_delta_a"] is not None
    assert abs(features["country_delta_a"] - expected_delta) < 0.001


def test_is_home() -> None:
    group = CountryPerformanceFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    # Player B is from GBR, tournament in GBR
    features = group.compute(state_a, state_b, _ctx(
        player_a_nationality="ESP", player_b_nationality="GBR",
    ))
    assert features["is_home_a"] == 0.0
    assert features["is_home_b"] == 1.0


def test_no_country_on_context() -> None:
    group = CountryPerformanceFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx(country_ioc=None))
    assert features["country_win_rate_a"] == 0.5  # default, no data
    assert features["country_matches_a"] == 0
    assert features["is_home_a"] == 0.0


def test_feature_names_count() -> None:
    group = CountryPerformanceFeatures()
    assert len(group.feature_names()) == 14
    assert group.name == "country_performance"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_country_performance.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement CountryPerformanceFeatures**

Create `src/teelo/features/groups/country_performance.py`:

```python
"""Country and region performance features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState
from teelo.utils.geo import ioc_to_region

def _career_win_rate(state: PlayerState) -> float:
    total = state.wins_total + state.losses_total
    if total == 0:
        return 0.5
    return state.wins_total / total


class CountryPerformanceFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "country_performance"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for suffix in ("a", "b"):
            for feat in (
                "country_win_rate",
                "country_matches",
                "country_delta",
                "region_win_rate",
                "region_matches",
                "region_delta",
                "is_home",
            ):
                names.append(f"{feat}_{suffix}")
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        country = ctx.tournament_country_ioc
        region = ioc_to_region(country) if country else None

        for suffix, state, nationality in (
            ("a", state_a, ctx.player_a_nationality),
            ("b", state_b, ctx.player_b_nationality),
        ):
            career_wr = _career_win_rate(state)

            # Country features
            if country and country in state.country_record:
                wins, losses = state.country_record[country]
                total = wins + losses
                features[f"country_matches_{suffix}"] = float(total)
                wr = wins / total if total > 0 else 0.5
                features[f"country_win_rate_{suffix}"] = wr
                features[f"country_delta_{suffix}"] = wr - career_wr
            else:
                features[f"country_win_rate_{suffix}"] = 0.5
                features[f"country_matches_{suffix}"] = 0.0
                features[f"country_delta_{suffix}"] = 0.0

            # Region features
            if region and region in state.region_record:
                wins, losses = state.region_record[region]
                total = wins + losses
                features[f"region_matches_{suffix}"] = float(total)
                wr = wins / total if total > 0 else 0.5
                features[f"region_win_rate_{suffix}"] = wr
                features[f"region_delta_{suffix}"] = wr - career_wr
            else:
                features[f"region_win_rate_{suffix}"] = 0.5
                features[f"region_matches_{suffix}"] = 0.0
                features[f"region_delta_{suffix}"] = 0.0

            # Is home
            if nationality and country:
                features[f"is_home_{suffix}"] = 1.0 if nationality == country else 0.0
            else:
                features[f"is_home_{suffix}"] = 0.0

        return features
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_country_performance.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/groups/country_performance.py tests/unit/test_country_performance.py
git commit -m "feat: implement CountryPerformanceFeatures group (country/region win rates, is_home)"
```

---

### Task 9: Extend context.py with calendar features

**Files:**
- Modify: `src/teelo/features/groups/context.py`
- Create: `tests/unit/test_calendar_features.py`

- [ ] **Step 1: Write tests**

Create `tests/unit/test_calendar_features.py`:

```python
import math
from datetime import date

from teelo.features.groups.context import ContextFeatures
from teelo.features.state import MatchContext, PlayerState


def _ctx(match_date: date | None = date(2026, 6, 15)) -> MatchContext:
    return MatchContext(
        match_id=1, match_date=match_date, surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_month_sin_cos_june() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    features = group.compute(state, state, _ctx(date(2026, 6, 15)))
    expected_sin = math.sin(2 * math.pi * 6 / 12)
    expected_cos = math.cos(2 * math.pi * 6 / 12)
    assert features["month_sin"] is not None
    assert abs(features["month_sin"] - expected_sin) < 0.001
    assert abs(features["month_cos"] - expected_cos) < 0.001


def test_year_progress_midyear() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    d = date(2026, 7, 2)  # day 183
    features = group.compute(state, state, _ctx(d))
    assert features["year_progress"] is not None
    assert abs(features["year_progress"] - 183 / 365) < 0.01


def test_calendar_none_when_no_date() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    features = group.compute(state, state, _ctx(match_date=None))
    assert features["month_sin"] is None
    assert features["month_cos"] is None
    assert features["year_progress"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_calendar_features.py -v`
Expected: FAIL — `KeyError: 'month_sin'`

- [ ] **Step 3: Add calendar features to context.py**

In `src/teelo/features/groups/context.py`:

Add import at top:

```python
import math
```

Add to `feature_names()` list (before the closing `]`):

```python
            "month_sin",
            "month_cos",
            "year_progress",
```

Add to `compute()` method (before `return features`):

```python
        # Calendar features
        if ctx.match_date is not None:
            month = ctx.match_date.month
            features["month_sin"] = math.sin(2 * math.pi * month / 12)
            features["month_cos"] = math.cos(2 * math.pi * month / 12)
            features["year_progress"] = ctx.match_date.timetuple().tm_yday / 365
        else:
            features["month_sin"] = None
            features["month_cos"] = None
            features["year_progress"] = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_calendar_features.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/groups/context.py tests/unit/test_calendar_features.py
git commit -m "feat: add calendar features (month_sin, month_cos, year_progress) to context group"
```

---

### Task 10: Extend elo.py with surface gap features

**Files:**
- Modify: `src/teelo/features/groups/elo.py`
- Create: `tests/unit/test_surface_gap.py`

- [ ] **Step 1: Write tests**

Create `tests/unit/test_surface_gap.py`:

```python
from datetime import date

from teelo.features.groups.elo import EloCoreFeatures
from teelo.features.state import MatchContext, PlayerState


def _ctx(surface: str | None = "Clay") -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface=surface,
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_surface_gap() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0
    state_a.surface_elo["Hard"] = 1580.0
    state_b = PlayerState(player_id=2, elo_current=1500.0)
    state_b.surface_elo["Clay"] = 1500.0

    features = group.compute(state_a, state_b, _ctx("Clay"))
    assert features["surface_gap_a"] == 50.0  # 1650 - 1600
    assert features["surface_gap_b"] == 0.0   # 1500 - 1500


def test_off_surface_elo() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0
    state_a.surface_elo["Hard"] = 1580.0
    state_a.surface_elo["Grass"] = 1560.0
    state_b = PlayerState(player_id=2)

    features = group.compute(state_a, state_b, _ctx("Clay"))
    # Off-surface = avg of Hard (1580) and Grass (1560) = 1570
    assert features["off_surface_elo_a"] == 1570.0


def test_surface_gap_none_when_no_surface() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx(surface=None))
    assert features["surface_gap_a"] is None
    assert features["off_surface_elo_a"] is None


def test_off_surface_elo_none_when_single_surface() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0  # only one surface
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx("Clay"))
    assert features["surface_gap_a"] == 50.0
    assert features["off_surface_elo_a"] is None  # no other surfaces
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_surface_gap.py -v`
Expected: FAIL — `KeyError: 'surface_gap_a'`

- [ ] **Step 3: Add surface gap features to EloCoreFeatures**

In `src/teelo/features/groups/elo.py`, in `EloCoreFeatures`:

Add to `feature_names()` list:

```python
            "surface_gap_a",
            "surface_gap_b",
            "off_surface_elo_a",
            "off_surface_elo_b",
```

Add to `compute()` method (before `return features`):

```python
        # Surface gap features
        for suffix, state in (("a", state_a), ("b", state_b)):
            if ctx.surface and ctx.surface in state.surface_elo:
                features[f"surface_gap_{suffix}"] = (
                    state.surface_elo[ctx.surface] - state.elo_current
                )
                other_elos = [
                    v for k, v in state.surface_elo.items() if k != ctx.surface
                ]
                features[f"off_surface_elo_{suffix}"] = (
                    sum(other_elos) / len(other_elos) if other_elos else None
                )
            else:
                features[f"surface_gap_{suffix}"] = None
                features[f"off_surface_elo_{suffix}"] = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_surface_gap.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/groups/elo.py tests/unit/test_surface_gap.py
git commit -m "feat: add surface_gap and off_surface_elo features to EloCoreFeatures"
```

---

### Task 11: Update registry — replace dominance with score_profile, add country_performance

**Files:**
- Modify: `src/teelo/features/__init__.py`
- Delete: `src/teelo/features/groups/dominance.py`

- [ ] **Step 1: Write test for new registry**

Add to `tests/unit/test_feature_registry_v2.py` (or create if needed):

```python
from teelo.features import build_registry


def test_baseline_v2_has_score_profile() -> None:
    registry = build_registry("baseline_v2")
    names = registry.all_feature_names()
    assert "game_diff_avg_8_a" in names  # inherited from dominance
    assert "comeback_rate_64_a" in names  # new W64
    assert "vs_clutch_win_rate_a" in names  # clutch matchup
    assert "country_win_rate_a" in names  # country performance


def test_baseline_v2_no_dominance_group() -> None:
    registry = build_registry("baseline_v2")
    group_names = [g.name for g in registry._groups]
    assert "dominance" not in group_names
    assert "score_profile" in group_names
    assert "country_performance" in group_names


def test_full_preset_no_new_groups() -> None:
    """Full preset should NOT include score_profile or country_performance."""
    registry = build_registry("full")
    group_names = [g.name for g in registry._groups]
    assert "score_profile" not in group_names
    assert "country_performance" not in group_names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_feature_registry_v2.py::test_baseline_v2_has_score_profile -v`
Expected: FAIL

- [ ] **Step 3: Update __init__.py**

In `src/teelo/features/__init__.py`:

Replace the dominance import (line 13):
```python
# Delete: from teelo.features.groups.dominance import DominanceFeatures
from teelo.features.groups.score_profile import ScoreProfileFeatures
from teelo.features.groups.country_performance import CountryPerformanceFeatures
```

In `build_registry()`, replace `DominanceFeatures()` registration (line 187):
```python
        registry.register(ScoreProfileFeatures())
        # Replace: registry.register(DominanceFeatures())
```

Add after `ConfidenceFeatures()` registration (line 190):
```python
        registry.register(CountryPerformanceFeatures())
```

- [ ] **Step 4: Delete dominance.py**

```bash
rm src/teelo/features/groups/dominance.py
```

- [ ] **Step 5: Run all tests**

Run: `pytest tests/ -v --timeout=30`
Expected: All PASS (any tests that imported DominanceFeatures directly will need updating)

- [ ] **Step 6: Fix any broken imports**

In `tests/unit/test_feature_groups_v2.py`, update DominanceFeatures references:

Replace:
```python
from teelo.features.groups.dominance import DominanceFeatures
```
With:
```python
from teelo.features.groups.score_profile import ScoreProfileFeatures
```

And replace all `DominanceFeatures()` constructor calls with `ScoreProfileFeatures()`. The W=8 feature names are preserved, so test assertions on feature values should still pass.

- [ ] **Step 7: Commit**

```bash
git add src/teelo/features/__init__.py tests/
git rm src/teelo/features/groups/dominance.py
git commit -m "feat: replace DominanceFeatures with ScoreProfileFeatures + CountryPerformanceFeatures in registry"
```

---

### Task 12: Remove min-sample None returns from existing feature groups

**Files:**
- Modify: `src/teelo/features/groups/form.py`
- Modify: `src/teelo/features/groups/elo.py`
- Modify: `src/teelo/features/groups/h2h.py`
- Modify: `src/teelo/features/groups/activity.py`
- Modify: `src/teelo/features/groups/opponent_quality.py`
- Modify: `src/teelo/features/groups/tournament_history.py`

**Design principle:** Features never return `None` due to insufficient sample size. When denominator is 0, return a neutral default. `None` is only for missing metadata (no surface, no date). The model uses count/confidence companion features to judge reliability.

**Defaults by type:**
- Win rates / proportions → `0.5`
- Diffs / averages / counts → `0.0`
- ELO momentum / variance → `0.0`
- ELO values → `1500.0`
- Ratios (peak_ratio) → `1.0`

- [ ] **Step 1: Update form.py**

Remove all min-sample checks. Changes:
- `_win_rate_in_window()`: Remove `if count < 5: return None, count` → return `0.5, count` when count is 0
- `_surface_win_rate()`: Remove `if total < 5: return None` → return `0.5` when total is 0
- `_level_win_rate()`: Remove `if total < 5: return None` → return `0.5` when total is 0
- `_career_win_rate()`: Remove `if total < 10: return None` → return `0.5` when total is 0

- [ ] **Step 2: Update elo.py**

- `EloHistoryFeatures`: `elo_momentum` — Remove `if len < 2: return None` → return `0.0` when history length < 2
- `EloVarianceFeatures`: `elo_var_*` — Remove `if len < 3: return None` → return `0.0` when insufficient history
- `EloCoreFeatures`: `peak_ratio` — Remove `if elo_peak <= 0: return None` → return `1.0`

- [ ] **Step 3: Update h2h.py**

- `h2h_a_dominance`: Remove `if h2h_total == 0: return None` → return `0.5`

- [ ] **Step 4: Update activity.py**

- `games_last_match`: Remove `return None` when no prior matches → return `0.0`

- [ ] **Step 5: Update opponent_quality.py**

- `_average()`: Remove `if not values: return None` → return `0.0` when empty (affects `opp_elo_avg`, `opp_surface_elo_avg`, `elo_overperf`)
- Note: `opp_elo_avg` returning `0.0` for an empty list vs `1500.0` is debatable. `0.0` is clearly a "no data" signal when combined with the sample count companion in confidence.py. Use `0.0`.
- `wins_vs_higher_elo` / `losses_vs_lower_elo`: Remove `return None` when no records → return `0.0`

- [ ] **Step 6: Update tournament_history.py**

- `tournament_win_rate`: Remove `if total < 2: return None` → return `0.5` when total is 0

- [ ] **Step 7: Update existing tests**

Any existing tests that assert `is None` for rate features with small samples need updating to assert the new default values instead. Run tests to find failures:

Run: `pytest tests/ -v --timeout=30`

Fix any assertions that expected `None` but now get a default value.

- [ ] **Step 8: Commit**

```bash
git add src/teelo/features/groups/ tests/
git commit -m "refactor: remove min-sample None returns from all feature groups — always return defaults"
```

---

### Task 13: Run full test suite and verify feature counts

**Files:**
- No new files

- [ ] **Step 1: Run full test suite**

Run: `pytest tests/ -v --timeout=60`
Expected: All PASS

- [ ] **Step 2: Verify feature counts**

```python
# Quick check in Python REPL or a test
from teelo.features import build_registry

reg = build_registry("baseline_v2")
names = reg.all_feature_names()
print(f"Total features: {len(names)}")
# Expected: ~235 (158 - 14 dominance + 56 score_profile + 14 country + 3 calendar + 4 surface_gap = 221)
# Note: the exact count depends on how presets handle the new features

# Verify no duplicate names
assert len(names) == len(set(names)), f"Duplicate features found: {set(n for n in names if names.count(n) > 1)}"
```

- [ ] **Step 3: Verify backward-compatible feature names**

```python
# All old dominance W=8 names must exist in the new registry
old_dominance_names = [
    "game_diff_avg_8_a", "game_diff_avg_8_b",
    "set_diff_avg_8_a", "set_diff_avg_8_b",
    "straight_sets_rate_8_a", "straight_sets_rate_8_b",
    "deciding_set_rate_8_a", "deciding_set_rate_8_b",
    "tiebreak_rate_8_a", "tiebreak_rate_8_b",
    "tiebreak_win_rate_8_a", "tiebreak_win_rate_8_b",
    "close_match_rate_8_a", "close_match_rate_8_b",
]
for name in old_dominance_names:
    assert name in names, f"Missing backward-compatible feature: {name}"
```

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "chore: verify Phase 1 feature expansion — all tests pass, feature counts correct"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add MatchRecord fields | `state.py`, tests |
| 2 | Add PlayerState + MatchContext fields | `state.py`, tests |
| 3 | Add region mapping to geo.py | `geo.py`, tests |
| 4 | Update PlayerState.update() for country tracking | `state.py`, tests |
| 5 | Engine: load country_ioc + nationalities | `engine.py` |
| 6 | Engine: first_set_lost, clutch/specialist helpers | `engine.py`, tests |
| 7 | Implement ScoreProfileFeatures | `score_profile.py`, tests |
| 8 | Implement CountryPerformanceFeatures | `country_performance.py`, tests |
| 9 | Extend context.py with calendar | `context.py`, tests |
| 10 | Extend elo.py with surface gap | `elo.py`, tests |
| 11 | Update registry, delete dominance | `__init__.py`, `dominance.py` |
| 12 | Remove min-sample None returns from existing groups | `form.py`, `elo.py`, `h2h.py`, `activity.py`, `opponent_quality.py`, `tournament_history.py` |
| 13 | Full verification | — |
