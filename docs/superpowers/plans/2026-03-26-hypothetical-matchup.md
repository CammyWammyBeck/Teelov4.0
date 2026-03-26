# Hypothetical Matchup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable users to predict hypothetical matchups between any two players, with full feature breakdowns, powered by persistent PlayerState snapshots and an incremental feature engine.

**Architecture:** Four phases — (1) ELO unification removes duplicate ELO computation from the feature engine, (2) PlayerState serialization + new DB tables for snapshots, (3) incremental engine that resumes from saved state, (4) web page with form, background prediction task, and result display.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0, PostgreSQL, XGBoost, Jinja2, Tailwind CSS, vanilla JS

**Spec:** `docs/superpowers/specs/2026-03-26-hypothetical-matchup-design.md`

---

## File Structure

### New files
- `src/teelo/features/serialization.py` — PlayerState to_dict/from_dict + MatchRecord/H2HRecord serialization
- `src/teelo/web/routers/matchup.py` — matchup form page + prediction API endpoints
- `src/teelo/web/templates/matchup.html` — form + loading + result template
- `src/teelo/web/static/js/matchup.js` — form interactions, polling, result rendering
- `alembic/versions/<auto>_add_player_feature_state_tables.py` — migration for new tables
- `tests/unit/test_state_serialization.py` — round-trip serialization tests
- `tests/unit/test_matchup_prediction.py` — prediction endpoint tests

### Modified files
- `src/teelo/db/models.py` — add `PlayerFeatureState` and `PlayerFeatureSnapshot` models
- `src/teelo/features/state.py` — remove ELO fields from `PlayerState`, update `update()` method
- `src/teelo/features/groups/elo.py` — read ELO from a lookup object instead of `PlayerState`
- `src/teelo/features/engine.py` — add snapshot writes, incremental resume, pass ELO lookup to compute
- `src/teelo/features/registry.py` — update `compute_all` signature to accept ELO lookup
- `src/teelo/web/main.py` — include matchup router, load ML model at startup
- `src/teelo/web/templates/base.html` — add "Matchup" to navigation

---

## Phase 1: ELO Unification

### Task 1: Create ELO lookup dataclass and update feature group signatures

**Files:**
- Create: `src/teelo/features/elo_lookup.py`
- Modify: `src/teelo/features/registry.py`
- Test: `tests/unit/test_elo_lookup.py`

- [ ] **Step 1: Write test for EloLookup dataclass**

Create `tests/unit/test_elo_lookup.py`:

```python
from teelo.features.elo_lookup import EloLookup


def test_elo_lookup_returns_defaults_for_unknown_player() -> None:
    lookup = EloLookup(elo={}, surface_elo={}, elo_history={})
    assert lookup.get_elo(999) == 1500.0
    assert lookup.get_surface_elo(999, "Hard") == 1500.0
    assert lookup.get_elo_peak(999) == 1500.0
    assert lookup.get_elo_history(999) == []


def test_elo_lookup_returns_stored_values() -> None:
    lookup = EloLookup(
        elo={1: 2100.0},
        surface_elo={1: {"Hard": 2150.0, "Clay": 2050.0}},
        elo_history={1: [(100, 2000.0), (200, 2100.0)]},
        elo_peak={1: 2200.0},
        surface_elo_peak={1: {"Hard": 2180.0}},
    )
    assert lookup.get_elo(1) == 2100.0
    assert lookup.get_surface_elo(1, "Hard") == 2150.0
    assert lookup.get_surface_elo(1, "Grass") == 1500.0
    assert lookup.get_elo_peak(1) == 2200.0
    assert lookup.get_elo_history(1) == [(100, 2000.0), (200, 2100.0)]
    assert lookup.get_surface_elo_peak(1, "Hard") == 2180.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && pytest tests/unit/test_elo_lookup.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'teelo.features.elo_lookup'`

- [ ] **Step 3: Implement EloLookup**

Create `src/teelo/features/elo_lookup.py`:

```python
"""ELO data lookup for feature computation.

Provides read-only access to ELO values computed by the ELO updater.
Replaces the previous approach of storing ELO in PlayerState.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EloLookup:
    """Read-only ELO data sourced from the ELO updater's persisted tables."""

    elo: dict[int, float] = field(default_factory=dict)
    surface_elo: dict[int, dict[str, float]] = field(default_factory=dict)
    elo_peak: dict[int, float] = field(default_factory=dict)
    surface_elo_peak: dict[int, dict[str, float]] = field(default_factory=dict)
    elo_history: dict[int, list[tuple[int, float]]] = field(default_factory=dict)

    def get_elo(self, player_id: int) -> float:
        return self.elo.get(player_id, 1500.0)

    def get_surface_elo(self, player_id: int, surface: str) -> float:
        return self.surface_elo.get(player_id, {}).get(surface, 1500.0)

    def get_elo_peak(self, player_id: int) -> float:
        return self.elo_peak.get(player_id, 1500.0)

    def get_surface_elo_peak(self, player_id: int, surface: str) -> float:
        return self.surface_elo_peak.get(player_id, {}).get(surface, 1500.0)

    def get_surface_elos(self, player_id: int) -> dict[str, float]:
        return self.surface_elo.get(player_id, {})

    def get_elo_history(self, player_id: int) -> list[tuple[int, float]]:
        return self.elo_history.get(player_id, [])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_elo_lookup.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/elo_lookup.py tests/unit/test_elo_lookup.py
git commit -m "feat: add EloLookup dataclass for unified ELO access"
```

---

### Task 2: Update feature groups to use EloLookup instead of PlayerState ELO fields

**Files:**
- Modify: `src/teelo/features/groups/elo.py`
- Modify: `src/teelo/features/registry.py` (FeatureGroup ABC and compute_all signature)
- Modify: all feature group files in `src/teelo/features/groups/` (add elo_lookup parameter)
- Test: `tests/unit/test_feature_groups_v2.py` (update existing tests)

- [ ] **Step 1: Update FeatureGroup ABC to accept EloLookup**

In `src/teelo/features/registry.py`, update the `FeatureGroup.compute()` abstract method signature to accept an optional `elo_lookup` parameter, and update `FeatureRegistry.compute_all()` to pass it through:

```python
# In FeatureGroup ABC:
@abc.abstractmethod
def compute(
    self,
    state_a: PlayerState,
    state_b: PlayerState,
    ctx: MatchContext,
    elo_lookup: EloLookup | None = None,
) -> dict[str, float | None]: ...

# In FeatureRegistry.compute_all():
def compute_all(
    self,
    state_a: PlayerState,
    state_b: PlayerState,
    ctx: MatchContext,
    elo_lookup: EloLookup | None = None,
) -> dict[str, float | None]:
    features: dict[str, float | None] = {}
    for group in self._groups:
        features.update(group.compute(state_a, state_b, ctx, elo_lookup=elo_lookup))
    return features
```

- [ ] **Step 2: Update all existing feature group compute() signatures**

Add `elo_lookup: EloLookup | None = None` parameter to every `compute()` method in:
- `src/teelo/features/groups/elo.py` — EloCoreFeatures, EloHistoryFeatures, EloVarianceFeatures
- `src/teelo/features/groups/form.py`
- `src/teelo/features/groups/h2h.py`
- `src/teelo/features/groups/activity.py`
- `src/teelo/features/groups/opponent_quality.py`
- `src/teelo/features/groups/score_profile.py`
- `src/teelo/features/groups/fatigue.py`
- `src/teelo/features/groups/tournament_history.py`
- `src/teelo/features/groups/confidence.py`
- `src/teelo/features/groups/country_performance.py`
- `src/teelo/features/groups/context.py`

Only the ELO groups will actually use the parameter; others just accept it for interface consistency.

- [ ] **Step 3: Rewrite ELO feature groups to use EloLookup**

In `src/teelo/features/groups/elo.py`, change `EloCoreFeatures.compute()` to read from `elo_lookup` instead of `state.elo_current`, `state.elo_peak`, `state.surface_elo`:

```python
def compute(
    self,
    state_a: PlayerState,
    state_b: PlayerState,
    ctx: MatchContext,
    elo_lookup: EloLookup | None = None,
) -> dict[str, float | None]:
    if elo_lookup is None:
        return {name: None for name in self.feature_names()}

    elo_a = elo_lookup.get_elo(state_a.player_id)
    elo_b = elo_lookup.get_elo(state_b.player_id)

    surface_elo_a = (
        elo_lookup.get_surface_elo(state_a.player_id, ctx.surface)
        if ctx.surface else None
    )
    surface_elo_b = (
        elo_lookup.get_surface_elo(state_b.player_id, ctx.surface)
        if ctx.surface else None
    )

    if surface_elo_a is None or surface_elo_b is None:
        surface_elo_diff: float | None = None
    else:
        surface_elo_diff = surface_elo_a - surface_elo_b

    peak_a = elo_lookup.get_elo_peak(state_a.player_id)
    peak_b = elo_lookup.get_elo_peak(state_b.player_id)
    peak_ratio_a = elo_a / peak_a if peak_a > 0 else 1.0
    peak_ratio_b = elo_b / peak_b if peak_b > 0 else 1.0

    features: dict[str, float | None] = {
        "elo_a": elo_a,
        "elo_b": elo_b,
        "elo_diff": elo_a - elo_b,
        "surface_elo_a": surface_elo_a,
        "surface_elo_b": surface_elo_b,
        "surface_elo_diff": surface_elo_diff,
        "peak_elo_a": peak_a,
        "peak_elo_b": peak_b,
        "peak_ratio_a": peak_ratio_a,
        "peak_ratio_b": peak_ratio_b,
    }

    # Surface gap features
    for suffix, pid in (("a", state_a.player_id), ("b", state_b.player_id)):
        player_surfaces = elo_lookup.get_surface_elos(pid)
        if ctx.surface and ctx.surface in player_surfaces:
            features[f"surface_gap_{suffix}"] = (
                player_surfaces[ctx.surface] - elo_lookup.get_elo(pid)
            )
            other_elos = [v for k, v in player_surfaces.items() if k != ctx.surface]
            features[f"off_surface_elo_{suffix}"] = (
                sum(other_elos) / len(other_elos) if other_elos else None
            )
        else:
            features[f"surface_gap_{suffix}"] = None
            features[f"off_surface_elo_{suffix}"] = None

    return features
```

Similarly update `EloHistoryFeatures` and `EloVarianceFeatures` to use `elo_lookup.get_elo_history()` instead of `state.elo_history`.

- [ ] **Step 4: Update existing tests**

In `tests/unit/test_feature_groups_v2.py`, update test helpers to pass `elo_lookup` when calling `compute()`. Create an `EloLookup` from the same data that was previously set on `PlayerState`.

- [ ] **Step 5: Run all tests**

Run: `pytest tests/unit/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: ELO feature groups read from EloLookup instead of PlayerState"
```

---

### Task 3: Remove ELO fields from PlayerState and update engine

**Files:**
- Modify: `src/teelo/features/state.py:70-104` — remove ELO fields and ELO update logic
- Modify: `src/teelo/features/engine.py:161-164,251-255,416-420` — build EloLookup from DB, pass to compute_all, remove specialist_score ELO dependency
- Test: existing tests should still pass

- [ ] **Step 1: Remove ELO fields from PlayerState**

In `src/teelo/features/state.py`, remove these fields from `PlayerState`:
- `elo_current: float = 1500.0` (line 72)
- `elo_peak: float = 1500.0` (line 73)
- `elo_history: deque[tuple[int, float]]` (line 74)
- `surface_elo: dict[str, float]` (line 75)
- `surface_elo_peak: dict[str, float]` (line 76)

And remove the ELO update logic from `update()` method (lines 97-104):
- `self.elo_current = elo_post`
- `self.elo_peak = max(self.elo_peak, elo_post)`
- `self.elo_history.append(...)`
- The `surface_elo` and `surface_elo_peak` updates

Also remove the `has_observed_surface_elo()` method (lines 154-157).

Change `update()` signature to remove `elo_post` and `surface_elo_post` parameters:
```python
def update(self, record: MatchRecord) -> None:
```

- [ ] **Step 2: Update engine to build EloLookup from DB data**

In `src/teelo/features/engine.py`:

1. Import `EloLookup` from `teelo.features.elo_lookup`
2. In `run()`, build an `EloLookup` that gets updated as the engine processes matches. The engine already loads `elo_pre_*`, `elo_post_*`, and surface ELO snapshots from the DB — use these to populate the lookup incrementally:

```python
# After loading surface snapshots, initialize EloLookup
elo_lookup = EloLookup()

# Inside the match loop, after reading elo_pre/post from DB columns:
# Update lookup with the latest known ELO for each player
if elo_post_a is not None:
    elo_lookup.elo[row.player_a_id] = elo_post_a
    elo_lookup.elo_peak[row.player_a_id] = max(
        elo_lookup.elo_peak.get(row.player_a_id, 1500.0), elo_post_a
    )
    elo_lookup.elo_history.setdefault(row.player_a_id, []).append(
        (row.temporal_order, elo_post_a)
    )
# Same for player B, and for surface ELO from snapshots
```

3. Pass `elo_lookup` to `registry.compute_all()`:
```python
features = self.registry.compute_all(state_a, state_b, ctx, elo_lookup=elo_lookup)
```

4. Update `state.update()` calls to remove ELO parameters:
```python
state_a.update(record_a)
state_b.update(record_b)
```

5. Update `_specialist_score()` to use `elo_lookup`:
```python
def _specialist_score(elo_lookup: EloLookup, player_id: int, surface: str | None) -> float | None:
    if surface is None:
        return None
    player_surfaces = elo_lookup.get_surface_elos(player_id)
    if surface not in player_surfaces:
        return None
    return player_surfaces[surface] - elo_lookup.get_elo(player_id)
```

- [ ] **Step 3: Run all tests**

Run: `pytest tests/unit/ -v`
Expected: PASS (may need to fix test helpers that set ELO on PlayerState)

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove ELO from PlayerState, engine reads from DB via EloLookup"
```

---

## Phase 2: PlayerState Serialization & DB Tables

### Task 4: Implement PlayerState serialization

**Files:**
- Create: `src/teelo/features/serialization.py`
- Create: `tests/unit/test_state_serialization.py`

- [ ] **Step 1: Write round-trip serialization tests**

Create `tests/unit/test_state_serialization.py`:

```python
from collections import deque, defaultdict
from datetime import date

from teelo.features.serialization import player_state_to_dict, player_state_from_dict
from teelo.features.state import H2HRecord, MatchRecord, PlayerState


def _sample_record() -> MatchRecord:
    return MatchRecord(
        temporal_order=2026010100001_70,
        won=True,
        surface="Hard",
        level_code="GS",
        games_won=18,
        games_lost=12,
        tournament_edition_id=100,
        tournament_id=50,
        match_date=date(2026, 1, 15),
        opponent_id=42,
        opponent_elo=2000.0,
        opponent_surface_elo=2050.0,
        expected_win_prob=0.55,
        sets_won=2,
        sets_lost=1,
        tiebreaks_played=1,
        tiebreaks_won=1,
        deciding_set_played=True,
        straight_sets=False,
        close_match=True,
        first_set_lost=True,
        opponent_clutch_score=0.48,
        opponent_specialist_score=30.0,
        country_ioc="AUS",
    )


def _sample_state() -> PlayerState:
    state = PlayerState(player_id=1)
    state.matches.append(_sample_record())
    state.wins_total = 1
    state.first_match_date = date(2026, 1, 15)
    state.last_match_date = date(2026, 1, 15)
    state.surface_wins["Hard"] = 1
    state.level_wins["GS"] = 1
    state.h2h[42].append(H2HRecord(
        temporal_order=2026010100001_70,
        won=True,
        surface="Hard",
        level_code="GS",
        match_date=date(2026, 1, 15),
    ))
    state.tournament_matches[50] = 1
    state.tournament_wins[50] = 1
    state.clutch_score = 0.52
    state.country_record["AUS"] = (1, 0)
    state.region_record["Oceania"] = (1, 0)
    return state


def test_round_trip_preserves_state() -> None:
    original = _sample_state()
    data = player_state_to_dict(original)
    restored = player_state_from_dict(data)

    assert restored.player_id == original.player_id
    assert restored.wins_total == original.wins_total
    assert restored.losses_total == original.losses_total
    assert restored.first_match_date == original.first_match_date
    assert restored.last_match_date == original.last_match_date
    assert restored.clutch_score == original.clutch_score
    assert len(restored.matches) == len(original.matches)
    assert restored.matches[0].won is True
    assert restored.matches[0].match_date == date(2026, 1, 15)
    assert restored.matches[0].opponent_clutch_score == 0.48
    assert dict(restored.surface_wins) == dict(original.surface_wins)
    assert dict(restored.level_wins) == dict(original.level_wins)
    assert len(restored.h2h[42]) == 1
    assert restored.h2h[42][0].won is True
    assert dict(restored.tournament_matches) == dict(original.tournament_matches)
    assert restored.country_record == original.country_record
    assert restored.region_record == original.region_record


def test_round_trip_empty_state() -> None:
    original = PlayerState(player_id=99)
    data = player_state_to_dict(original)
    restored = player_state_from_dict(data)
    assert restored.player_id == 99
    assert len(restored.matches) == 0
    assert restored.wins_total == 0


def test_serialized_output_is_json_compatible() -> None:
    import json
    state = _sample_state()
    data = player_state_to_dict(state)
    # Must be JSON-serializable (no date objects, no tuples as keys, etc.)
    json_str = json.dumps(data)
    assert isinstance(json_str, str)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_state_serialization.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement serialization**

Create `src/teelo/features/serialization.py`:

```python
"""Serialize/deserialize PlayerState to/from JSON-compatible dicts."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import date
from typing import Any

from teelo.features.state import H2HRecord, MatchRecord, PlayerState


def player_state_to_dict(state: PlayerState) -> dict[str, Any]:
    return {
        "player_id": state.player_id,
        "matches": [_match_record_to_dict(r) for r in state.matches],
        "first_match_date": state.first_match_date.isoformat() if state.first_match_date else None,
        "last_match_date": state.last_match_date.isoformat() if state.last_match_date else None,
        "wins_total": state.wins_total,
        "losses_total": state.losses_total,
        "surface_wins": dict(state.surface_wins),
        "surface_losses": dict(state.surface_losses),
        "level_wins": dict(state.level_wins),
        "level_losses": dict(state.level_losses),
        "h2h": {
            str(k): [_h2h_record_to_dict(r) for r in v]
            for k, v in state.h2h.items()
        },
        "tournament_matches": {str(k): v for k, v in state.tournament_matches.items()},
        "tournament_wins": {str(k): v for k, v in state.tournament_wins.items()},
        "tournament_losses": {str(k): v for k, v in state.tournament_losses.items()},
        "clutch_score": state.clutch_score,
        "country_record": {k: list(v) for k, v in state.country_record.items()},
        "region_record": {k: list(v) for k, v in state.region_record.items()},
    }


def player_state_from_dict(data: dict[str, Any]) -> PlayerState:
    state = PlayerState(player_id=data["player_id"])
    state.matches = deque(
        (_match_record_from_dict(r) for r in data.get("matches", [])),
        maxlen=1024,
    )
    state.first_match_date = date.fromisoformat(data["first_match_date"]) if data.get("first_match_date") else None
    state.last_match_date = date.fromisoformat(data["last_match_date"]) if data.get("last_match_date") else None
    state.wins_total = data.get("wins_total", 0)
    state.losses_total = data.get("losses_total", 0)
    state.surface_wins = defaultdict(int, data.get("surface_wins", {}))
    state.surface_losses = defaultdict(int, data.get("surface_losses", {}))
    state.level_wins = defaultdict(int, data.get("level_wins", {}))
    state.level_losses = defaultdict(int, data.get("level_losses", {}))
    state.h2h = defaultdict(list, {
        int(k): [_h2h_record_from_dict(r) for r in v]
        for k, v in data.get("h2h", {}).items()
    })
    state.tournament_matches = defaultdict(int, {int(k): v for k, v in data.get("tournament_matches", {}).items()})
    state.tournament_wins = defaultdict(int, {int(k): v for k, v in data.get("tournament_wins", {}).items()})
    state.tournament_losses = defaultdict(int, {int(k): v for k, v in data.get("tournament_losses", {}).items()})
    state.clutch_score = data.get("clutch_score")
    state.country_record = {k: tuple(v) for k, v in data.get("country_record", {}).items()}
    state.region_record = {k: tuple(v) for k, v in data.get("region_record", {}).items()}
    return state


def _match_record_to_dict(r: MatchRecord) -> dict[str, Any]:
    return {
        "temporal_order": r.temporal_order,
        "won": r.won,
        "surface": r.surface,
        "level_code": r.level_code,
        "games_won": r.games_won,
        "games_lost": r.games_lost,
        "tournament_edition_id": r.tournament_edition_id,
        "tournament_id": r.tournament_id,
        "match_date": r.match_date.isoformat() if r.match_date else None,
        "opponent_id": r.opponent_id,
        "opponent_elo": r.opponent_elo,
        "opponent_surface_elo": r.opponent_surface_elo,
        "expected_win_prob": r.expected_win_prob,
        "sets_won": r.sets_won,
        "sets_lost": r.sets_lost,
        "tiebreaks_played": r.tiebreaks_played,
        "tiebreaks_won": r.tiebreaks_won,
        "deciding_set_played": r.deciding_set_played,
        "straight_sets": r.straight_sets,
        "close_match": r.close_match,
        "first_set_lost": r.first_set_lost,
        "opponent_clutch_score": r.opponent_clutch_score,
        "opponent_specialist_score": r.opponent_specialist_score,
        "country_ioc": r.country_ioc,
    }


def _match_record_from_dict(d: dict[str, Any]) -> MatchRecord:
    return MatchRecord(
        temporal_order=d["temporal_order"],
        won=d["won"],
        surface=d.get("surface"),
        level_code=d["level_code"],
        games_won=d["games_won"],
        games_lost=d["games_lost"],
        tournament_edition_id=d.get("tournament_edition_id"),
        tournament_id=d.get("tournament_id"),
        match_date=date.fromisoformat(d["match_date"]) if d.get("match_date") else None,
        opponent_id=d["opponent_id"],
        opponent_elo=d.get("opponent_elo"),
        opponent_surface_elo=d.get("opponent_surface_elo"),
        expected_win_prob=d.get("expected_win_prob"),
        sets_won=d.get("sets_won", 0),
        sets_lost=d.get("sets_lost", 0),
        tiebreaks_played=d.get("tiebreaks_played", 0),
        tiebreaks_won=d.get("tiebreaks_won", 0),
        deciding_set_played=d.get("deciding_set_played", False),
        straight_sets=d.get("straight_sets", False),
        close_match=d.get("close_match", False),
        first_set_lost=d.get("first_set_lost", False),
        opponent_clutch_score=d.get("opponent_clutch_score"),
        opponent_specialist_score=d.get("opponent_specialist_score"),
        country_ioc=d.get("country_ioc"),
    )


def _h2h_record_to_dict(r: H2HRecord) -> dict[str, Any]:
    return {
        "temporal_order": r.temporal_order,
        "won": r.won,
        "surface": r.surface,
        "level_code": r.level_code,
        "match_date": r.match_date.isoformat() if r.match_date else None,
    }


def _h2h_record_from_dict(d: dict[str, Any]) -> H2HRecord:
    return H2HRecord(
        temporal_order=d["temporal_order"],
        won=d["won"],
        surface=d.get("surface"),
        level_code=d["level_code"],
        match_date=date.fromisoformat(d["match_date"]) if d.get("match_date") else None,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_state_serialization.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/serialization.py tests/unit/test_state_serialization.py
git commit -m "feat: add PlayerState serialization for snapshot persistence"
```

---

### Task 5: Add database models and migration for snapshot tables

**Files:**
- Modify: `src/teelo/db/models.py` — add `PlayerFeatureState` and `PlayerFeatureSnapshot`
- Create: Alembic migration via `alembic revision --autogenerate`

- [ ] **Step 1: Add models to models.py**

Add after the `MatchFeatures` model (around line 1002):

```python
class PlayerFeatureState(Base):
    """Current PlayerState per player — resume point for incremental engine."""

    __tablename__ = "player_feature_states"
    __table_args__ = (
        UniqueConstraint("player_id", "feature_set_id", name="uq_player_feature_state"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False, index=True)
    feature_set_id: Mapped[int] = mapped_column(ForeignKey("feature_sets.id"), nullable=False)
    temporal_order: Mapped[int] = mapped_column(nullable=False)
    state_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class PlayerFeatureSnapshot(Base):
    """PlayerState at each match — for historical lookups."""

    __tablename__ = "player_feature_snapshots"
    __table_args__ = (
        UniqueConstraint("player_id", "match_id", "feature_set_id", name="uq_player_feature_snapshot"),
        Index("ix_player_feature_snapshot_lookup", "player_id", "temporal_order", "feature_set_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"), nullable=False, index=True)
    feature_set_id: Mapped[int] = mapped_column(ForeignKey("feature_sets.id"), nullable=False)
    temporal_order: Mapped[int] = mapped_column(nullable=False)
    state_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
```

- [ ] **Step 2: Generate Alembic migration**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && alembic revision --autogenerate -m "add player feature state and snapshot tables"`

- [ ] **Step 3: Review the generated migration file**

Read the migration file and verify it creates the two tables with correct columns, constraints, and indexes.

- [ ] **Step 4: Apply migration**

Run: `alembic upgrade head`

- [ ] **Step 5: Commit**

```bash
git add src/teelo/db/models.py alembic/versions/
git commit -m "feat: add player_feature_states and player_feature_snapshots tables"
```

---

## Phase 3: Incremental Feature Engine

### Task 6: Add snapshot writing to the engine

**Files:**
- Modify: `src/teelo/features/engine.py`

- [ ] **Step 1: Add snapshot batch list and flush method**

In `FeatureEngine.__init__()`, add:
```python
self._state_batch: list[dict[str, Any]] = []
```

Add a new method `_flush_state_batch()` modeled on `_flush_batch()`:
```python
def _flush_state_batch(self, session: Session, feature_set_id: int) -> None:
    if not self._state_batch:
        return

    # Upsert per-match snapshots
    snapshot_rows = [r for r in self._state_batch if "match_id" in r]
    if snapshot_rows:
        stmt = insert(PlayerFeatureSnapshot).values(snapshot_rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_player_feature_snapshot",
            set_={
                "state_json": stmt.excluded.state_json,
                "temporal_order": stmt.excluded.temporal_order,
            },
        )
        session.execute(stmt)

    logger.info("feature_engine.state_batch_flushed", rows=len(self._state_batch))
    self._state_batch.clear()
```

- [ ] **Step 2: Write snapshots after state.update() in the main loop**

After `state_a.update(record_a)` and `state_b.update(record_b)` (around line 252), add:

```python
for pid, state in ((row.player_a_id, state_a), (row.player_b_id, state_b)):
    self._state_batch.append({
        "player_id": pid,
        "match_id": row.id,
        "feature_set_id": feature_set.id,
        "temporal_order": row.temporal_order,
        "state_json": player_state_to_dict(state),
    })
```

Add a flush trigger alongside the existing feature batch flush:
```python
if len(self._state_batch) >= BATCH_SIZE:
    self._flush_state_batch(session, feature_set.id)
```

And flush at the end of the loop alongside the feature flush.

- [ ] **Step 3: Write current-state rows at the end of the run**

After the main loop, upsert `PlayerFeatureState` for all players in `self.player_states`:

```python
def _flush_current_states(self, session: Session, feature_set_id: int) -> None:
    rows = []
    for pid, state in self.player_states.items():
        last_match = state.matches[-1] if state.matches else None
        if last_match is None:
            continue
        rows.append({
            "player_id": pid,
            "feature_set_id": feature_set_id,
            "temporal_order": last_match.temporal_order,
            "state_json": player_state_to_dict(state),
        })

    if not rows:
        return

    # Batch in chunks to avoid huge single INSERT
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        stmt = insert(PlayerFeatureState).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_player_feature_state",
            set_={
                "state_json": stmt.excluded.state_json,
                "temporal_order": stmt.excluded.temporal_order,
                "updated_at": func.now(),
            },
        )
        session.execute(stmt)

    logger.info("feature_engine.current_states_flushed", players=len(rows))
```

- [ ] **Step 4: Gate snapshot writes — only write for new matches**

Load existing snapshot match IDs at startup (similar to `_load_existing_feature_match_ids`):

```python
def _load_existing_snapshot_match_ids(self, session: Session, feature_set_id: int) -> set[int]:
    rows = session.execute(
        select(PlayerFeatureSnapshot.match_id).where(
            PlayerFeatureSnapshot.feature_set_id == feature_set_id
        ).distinct()
    ).scalars().all()
    return set(rows)
```

Only append to `_state_batch` if `row.id not in existing_snapshot_ids`.

- [ ] **Step 5: Run existing tests**

Run: `pytest tests/unit/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/teelo/features/engine.py
git commit -m "feat: engine writes PlayerState snapshots per match and current states"
```

---

### Task 7: Add incremental resume to the engine

**Files:**
- Modify: `src/teelo/features/engine.py`

- [ ] **Step 1: Add state loading method**

```python
def _load_player_states(
    self, session: Session, feature_set_id: int
) -> tuple[dict[int, PlayerState], int | None]:
    """Load persisted player states. Returns states dict and max temporal_order."""
    rows = session.execute(
        select(PlayerFeatureState).where(
            PlayerFeatureState.feature_set_id == feature_set_id
        )
    ).scalars().all()

    if not rows:
        return {}, None

    states: dict[int, PlayerState] = {}
    max_temporal = 0
    for row in rows:
        states[row.player_id] = player_state_from_dict(row.state_json)
        max_temporal = max(max_temporal, row.temporal_order)

    logger.info(
        "feature_engine.states_loaded",
        players=len(states),
        max_temporal_order=max_temporal,
    )
    return states, max_temporal
```

- [ ] **Step 1b: Add schema_version to FeatureSet model**

In `src/teelo/db/models.py`, add a `schema_version` column to `FeatureSet`:
```python
schema_version: Mapped[int] = mapped_column(default=1, server_default="1")
```

Generate and apply an Alembic migration for this column addition.

In `FeatureEngine._get_or_create_feature_set()`, compute a schema hash from `self.registry.all_feature_names()` and store it. On subsequent runs, compare the stored hash against the current registry — if different, log a warning and force `backfill=True`.

- [ ] **Step 2: Modify run() to use incremental resume**

At the start of `run()`, after getting feature_set:

```python
if not backfill:
    loaded_states, resume_point = self._load_player_states(session, feature_set.id)
    if loaded_states:
        self.player_states = loaded_states
        # Only load matches after resume point
        match_rows = self._load_matches(session, after_temporal_order=resume_point)
    else:
        match_rows = self._load_matches(session)
else:
    # Full backfill — replay everything
    match_rows = self._load_matches(session)
```

- [ ] **Step 3: Add temporal_order filter to _load_matches**

```python
def _load_matches(self, session: Session, after_temporal_order: int | None = None) -> list[Any]:
    stmt = (
        select(...)  # existing columns
        .select_from(Match)
        .join(TournamentEdition, ...)
        .join(Tournament, ...)
        .where(Match.temporal_order.is_not(None))
        .order_by(Match.temporal_order.asc())
    )
    if after_temporal_order is not None:
        stmt = stmt.where(Match.temporal_order > after_temporal_order)
    return list(session.execute(stmt).all())
```

- [ ] **Step 4: Run existing tests**

Run: `pytest tests/unit/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/engine.py
git commit -m "feat: incremental feature engine resumes from persisted PlayerState"
```

---

## Phase 4: Hypothetical Matchup Web Page

### Task 8: Create matchup router with prediction endpoint

**Files:**
- Create: `src/teelo/web/routers/matchup.py`
- Modify: `src/teelo/web/main.py` — include router, load model at startup

- [ ] **Step 1: Create the matchup router**

Create `src/teelo/web/routers/matchup.py`:

```python
"""Hypothetical matchup prediction endpoints."""

from __future__ import annotations

import uuid
import threading
from datetime import date, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from teelo.db.models import (
    Match,
    Player,
    PlayerEloState,
    PlayerFeatureSnapshot,
    PlayerFeatureState,
    PlayerSurfaceEloState,
)
from teelo.db.session import get_db
from teelo.features.elo_lookup import EloLookup
from teelo.features.registry import FeatureRegistry
from teelo.features.serialization import player_state_from_dict
from teelo.features.state import MatchContext, PlayerState
from teelo.web.services.feature_display import build_feature_groups, format_feature_value

logger = structlog.get_logger(__name__)

router = APIRouter()

# In-memory task store
_tasks: dict[str, dict[str, Any]] = {}
_tasks_lock = threading.Lock()


class MatchupRequest(BaseModel):
    player_a_id: int
    player_b_id: int
    date: date | None = None
    surface: str | None = None
    level_code: str | None = None
    tour: str | None = None
    round: str | None = None
    seed_a: int | None = None
    seed_b: int | None = None
    country_ioc: str | None = None


@router.get("/matchup", response_class=HTMLResponse)
async def matchup_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "matchup.html",
        {"request": request, "now": datetime.utcnow(), "current_path": "/matchup"},
    )


@router.post("/api/matchup/predict")
async def start_prediction(req: MatchupRequest, request: Request, db: Session = Depends(get_db)):
    task_id = str(uuid.uuid4())[:8]

    # Load player names for display
    player_a = db.execute(select(Player).where(Player.id == req.player_a_id)).scalar_one_or_none()
    player_b = db.execute(select(Player).where(Player.id == req.player_b_id)).scalar_one_or_none()
    if not player_a or not player_b:
        return JSONResponse({"status": "error", "message": "Player not found"}, status_code=404)

    with _tasks_lock:
        _tasks[task_id] = {
            "status": "pending",
            "player_a": {"id": player_a.id, "name": player_a.canonical_name, "nationality": player_a.nationality_ioc},
            "player_b": {"id": player_b.id, "name": player_b.canonical_name, "nationality": player_b.nationality_ioc},
        }

    # Run prediction in background thread
    thread = threading.Thread(
        target=_run_prediction,
        args=(task_id, req, request.app),
        daemon=True,
    )
    thread.start()

    return {"task_id": task_id}


@router.get("/api/matchup/predict/{task_id}")
async def poll_prediction(task_id: str):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if task is None:
        return JSONResponse({"status": "error", "message": "Task not found"}, status_code=404)
    return task


def _run_prediction(task_id: str, req: MatchupRequest, app: Any) -> None:
    """Background thread: load states, compute features, run inference."""
    try:
        from teelo.db.session import get_session

        with get_session() as db:
            # 1. Load player states
            state_a, state_b = _load_player_states(db, req)

            # 2. Load ELO values
            elo_lookup = _build_elo_lookup(db, req)

            # 3. Build MatchContext
            ctx = MatchContext(
                match_id=0,  # Hypothetical — no real match
                match_date=req.date or date.today(),
                surface=req.surface,
                level_code=req.level_code or "",
                tour=req.tour,
                gender=None,
                round=req.round,
                year=(req.date or date.today()).year,
                seed_a=req.seed_a,
                seed_b=req.seed_b,
                temporal_order=None,
                tournament_edition_id=None,
                tournament_country_ioc=req.country_ioc,
                player_a_nationality=None,  # Loaded from player
                player_b_nationality=None,
            )
            # Fill nationalities from player data
            with _tasks_lock:
                task_data = _tasks[task_id]
            ctx.player_a_nationality = task_data["player_a"].get("nationality")
            ctx.player_b_nationality = task_data["player_b"].get("nationality")

            # 4. Compute features
            registry: FeatureRegistry = app.state.feature_registry
            features = registry.compute_all(state_a, state_b, ctx, elo_lookup=elo_lookup)

            # 5. Run inference with A/B averaging
            model = app.state.prediction_model
            feature_names = app.state.prediction_feature_names
            prediction_a = _predict_with_averaging(model, features, feature_names)

            # 6. Build feature groups for display
            grouped = registry.grouped_features()
            neutral = registry.neutral_groups()
            feature_groups = build_feature_groups(features, grouped, neutral)

            with _tasks_lock:
                _tasks[task_id].update({
                    "status": "complete",
                    "result": {
                        "prediction_a": round(prediction_a, 4),
                        "features": feature_groups,
                        "elo_a": elo_lookup.get_elo(req.player_a_id),
                        "elo_b": elo_lookup.get_elo(req.player_b_id),
                        "surface_elo_a": elo_lookup.get_surface_elo(req.player_a_id, req.surface) if req.surface else None,
                        "surface_elo_b": elo_lookup.get_surface_elo(req.player_b_id, req.surface) if req.surface else None,
                        "inputs": {
                            "surface": req.surface,
                            "level_code": req.level_code,
                            "round": req.round,
                            "date": req.date.isoformat() if req.date else None,
                            "country_ioc": req.country_ioc,
                        },
                    },
                })

    except Exception as e:
        logger.error("matchup_prediction.failed", task_id=task_id, error=str(e))
        with _tasks_lock:
            _tasks[task_id] = {"status": "error", "message": str(e)}


def _load_player_states(db: Session, req: MatchupRequest) -> tuple[PlayerState, PlayerState]:
    """Load player feature states for the requested date."""
    states = []
    for pid in (req.player_a_id, req.player_b_id):
        # Use the active feature_set_id from app.state or look it up
        feature_set_id = _get_active_feature_set_id(db)

        if req.date and req.date < date.today():
            # Historical: find snapshot nearest to date via match table join
            row = db.execute(
                select(PlayerFeatureSnapshot.state_json)
                .join(Match, Match.id == PlayerFeatureSnapshot.match_id)
                .where(PlayerFeatureSnapshot.player_id == pid)
                .where(PlayerFeatureSnapshot.feature_set_id == feature_set_id)
                .where(Match.match_date <= req.date)
                .order_by(PlayerFeatureSnapshot.temporal_order.desc())
                .limit(1)
            ).scalar_one_or_none()
        else:
            # Current: load from player_feature_states
            row = db.execute(
                select(PlayerFeatureState.state_json)
                .where(PlayerFeatureState.player_id == pid)
                .where(PlayerFeatureState.feature_set_id == feature_set_id)
                .order_by(PlayerFeatureState.temporal_order.desc())
                .limit(1)
            ).scalar_one_or_none()

        if row is not None:
            states.append(player_state_from_dict(row))
        else:
            states.append(PlayerState(player_id=pid))

    return states[0], states[1]


def _build_elo_lookup(db: Session, req: MatchupRequest) -> EloLookup:
    """Build EloLookup from persisted ELO state tables."""
    lookup = EloLookup()

    for pid in (req.player_a_id, req.player_b_id):
        elo_state = db.execute(
            select(PlayerEloState).where(PlayerEloState.player_id == pid)
        ).scalar_one_or_none()
        if elo_state:
            lookup.elo[pid] = float(elo_state.elo_rating)
            lookup.elo_peak[pid] = float(elo_state.career_peak)

        surface_rows = db.execute(
            select(PlayerSurfaceEloState).where(PlayerSurfaceEloState.player_id == pid)
        ).scalars().all()
        if surface_rows:
            lookup.surface_elo[pid] = {r.surface: float(r.elo_rating) for r in surface_rows}
            lookup.surface_elo_peak[pid] = {r.surface: float(r.career_peak) for r in surface_rows}

    return lookup


def _predict_with_averaging(model: Any, features: dict, feature_names: list[str]) -> float:
    """Symmetric A/B prediction averaging."""
    import numpy as np
    import pandas as pd

    # Original orientation
    row = {name: features.get(name) for name in feature_names}
    df = pd.DataFrame([row])
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    prob_a_original = float(model.predict_proba(df)[0][1])

    # Swapped orientation: use existing swap utility
    from teelo.ml.randomize import swap_ab_features
    swapped = swap_ab_features(features)

    row_swapped = {name: swapped.get(name) for name in feature_names}
    df_swapped = pd.DataFrame([row_swapped])
    for col in df_swapped.columns:
        df_swapped[col] = pd.to_numeric(df_swapped[col], errors="coerce")
    prob_a_swapped = 1.0 - float(model.predict_proba(df_swapped)[0][1])

    return (prob_a_original + prob_a_swapped) / 2.0
```

- [ ] **Step 2: Register router and load model at startup**

In `src/teelo/web/main.py`, add:

```python
from teelo.web.routers.matchup import router as matchup_router

app.include_router(matchup_router)
```

Add model loading in the startup event (or `lifespan`). Use the existing versioning module:

```python
import joblib
import json
from pathlib import Path

from teelo.ml.versioning import latest_model_path
from teelo.features import build_registry, latest_preset

# Load prediction model at startup (same pattern as teelo.ml.predictor)
model_path_str = latest_model_path()
model_path = Path(model_path_str)
if model_path.exists():
    app.state.prediction_model = joblib.load(model_path)
    meta_path = model_path.with_suffix(".meta.json")
    with open(meta_path) as f:
        meta = json.load(f)
    app.state.prediction_feature_names = meta["feature_names"]

# Load feature registry
app.state.feature_registry = build_registry(latest_preset())
```

Check the exact metadata file naming convention by reading `src/teelo/ml/predictor.py` and `src/teelo/ml/trainer.py` — the meta file may be `.meta.json` or `_metadata.json`. Match whatever the trainer writes.

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/routers/matchup.py src/teelo/web/main.py
git commit -m "feat: add matchup prediction API with background task execution"
```

---

### Task 9: Create the matchup page template

**Files:**
- Create: `src/teelo/web/templates/matchup.html`
- Modify: `src/teelo/web/templates/base.html` — add nav link

- [ ] **Step 1: Create matchup.html template**

Create `src/teelo/web/templates/matchup.html` with:
- Form section with player autocomplete inputs, context dropdowns, date picker
- Loading/predicting section (initially hidden)
- Result section (initially hidden)
- All using existing Tailwind design tokens (teelo-lime, teelo-dark, bg-surface, etc.)

The template structure should follow the mockup from the design phase. Key elements:
- Player search inputs that call `/api/players/search?q=...`
- ELO badges that appear when a player is selected
- Optional fields grid: surface, level, round, date, tour, seeds, country
- "Predict Match" button
- All three states controlled by JS class toggling

- [ ] **Step 2: Add nav link**

In `src/teelo/web/templates/base.html`, add a "Matchup" link to the navigation alongside existing links (Matches, Rankings, Tournaments, etc.).

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/templates/matchup.html src/teelo/web/templates/base.html
git commit -m "feat: add matchup page template with form, loading, and result states"
```

---

### Task 10: Create matchup.js for form interactions and result rendering

**Files:**
- Create: `src/teelo/web/static/js/matchup.js`

- [ ] **Step 1: Implement player search autocomplete**

Debounced fetch to `/api/players/search?q=...&limit=8`, dropdown results, click-to-select populating a hidden `player_id` input and showing the player name + ELO badge.

- [ ] **Step 2: Implement form submission and polling**

On "Predict Match" click:
1. Collect form data into MatchupRequest JSON
2. POST to `/api/matchup/predict`
3. Transition to predicting state (hide form, show loading animation)
4. Poll `GET /api/matchup/predict/{task_id}` every 500ms
5. On `status: "complete"`, transition to result state
6. On `status: "error"`, show error message and re-show form

- [ ] **Step 3: Implement loading animation**

Player names with "vs", animated pulsing bars, rotating status text cycling through: "Crunching features...", "Analyzing form...", "Comparing surface ELO...", "Checking head-to-head...", "Running model..."

- [ ] **Step 4: Implement result rendering**

Build the result DOM from the JSON response:
- Prediction hero bar with percentages
- Context banner showing specified inputs
- "Hypothetical Matchup" label
- Feature group sections (collapsible, same visual structure as match_detail.html)
- "Modify Inputs & Re-predict" button

Use the same value formatting logic as match detail: rates as percentages, integers without decimals, floats to 1dp, null as "—". Color-code paired features (higher = green, lower = red) unless the group is neutral.

- [ ] **Step 5: Commit**

```bash
git add src/teelo/web/static/js/matchup.js
git commit -m "feat: add matchup.js with autocomplete, polling, and result rendering"
```

---

### Task 11: Rebuild Tailwind and end-to-end test

**Files:**
- Modify: `src/teelo/web/static/css/styles.css` (rebuilt)

- [ ] **Step 1: Rebuild Tailwind CSS**

Run: `npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify`

- [ ] **Step 2: Run the dev server and test manually**

Run: `uvicorn teelo.web.main:app --reload`

Test the flow:
1. Navigate to `/matchup`
2. Search and select two players
3. Optionally set surface/level/round
4. Click "Predict Match"
5. Verify loading animation appears
6. Verify prediction result displays with feature breakdowns
7. Click "Modify Inputs" and verify form re-appears
8. Test with a past date and verify historical state is used

- [ ] **Step 3: Run all tests**

Run: `pytest tests/unit/ -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/teelo/web/static/css/styles.css
git commit -m "chore: rebuild Tailwind CSS for matchup page"
```

---

### Task 12: Initial backfill of snapshots

This task runs the feature engine in backfill mode to populate the snapshot tables for the first time.

- [ ] **Step 1: Run full backfill**

The engine already has a `__main__` block with argparse at `engine.py:500`. Use it:

```bash
cd /home/cammybeck/Documents/programming/Teelov4.0
source venv/bin/activate
python src/teelo/features/engine.py --backfill
```

This will replay all matches and write both feature rows and PlayerState snapshots.

- [ ] **Step 2: Verify snapshots were written**

Check row counts:
```sql
SELECT COUNT(*) FROM player_feature_states;
SELECT COUNT(*) FROM player_feature_snapshots;
```

Expected: player_feature_states should have one row per player, player_feature_snapshots should have ~200K rows (2 × number of completed matches).

- [ ] **Step 3: Test incremental mode**

Run the engine again in normal (non-backfill) mode:
```bash
python -m teelo.features.engine
```

Expected: Should load states from DB, find no new matches, and finish quickly (seconds, not minutes).
