"""Tests for the match detail feature-importance integration."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from teelo.db.models import (
    Base,
    FeatureSet,
    Match,
    MatchFeatures,
    Player,
    Tournament,
    TournamentEdition,
)
from teelo.web.services.feature_display import build_row_pp_lookup
from teelo.web.services.match_service import select_match_features_for_view


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(_element, _compiler, **_kwargs):
    return "JSON"


def _build_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed_minimal_match(session, *, match_id: int = 1) -> None:
    player_a = Player(id=1, canonical_name="Alice")
    player_b = Player(id=2, canonical_name="Bob")
    tournament = Tournament(
        id=1,
        tournament_code="test-open",
        name="Test Open",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
    )
    edition = TournamentEdition(id=1, tournament_id=1, year=2026)
    match = Match(
        id=match_id,
        source="test",
        player_a_id=1,
        player_b_id=2,
        tournament_edition_id=1,
        status="scheduled",
    )
    session.add_all([player_a, player_b, tournament, edition, match])
    session.commit()


def test_build_row_pp_lookup_empty_when_missing() -> None:
    assert build_row_pp_lookup(None) == {}
    assert build_row_pp_lookup({}) == {}


def test_build_row_pp_lookup_includes_paired_and_raw() -> None:
    explanation = {
        "paired_rows": [
            {"key": "elo", "display_name": "ELO", "pp": 5.3},
            {"key": "h2h_wins", "display_name": "H2H Wins", "pp": -1.2},
        ],
        "contributions": {
            "elo_a": 3.1,
            "elo_b": 2.2,
            "surface_hard": 0.7,
            "elo_diff": 1.4,
        },
    }
    lookup = build_row_pp_lookup(explanation)
    # Paired base keys come from paired_rows
    assert lookup["elo"] == 5.3
    assert lookup["h2h_wins"] == -1.2
    # Raw singles come from contributions
    assert lookup["surface_hard"] == 0.7
    assert lookup["elo_diff"] == 1.4
    # Raw _a/_b entries are still available for direct lookup
    assert lookup["elo_a"] == 3.1


def test_select_match_features_prefers_explanation_feature_set() -> None:
    session = _build_session()
    _seed_minimal_match(session)

    fs_old = FeatureSet(
        id=1,
        name="baseline_v1",
        version="1",
        feature_definitions={"elo_a": {}, "elo_b": {}},
    )
    fs_new = FeatureSet(
        id=2,
        name="baseline_v2",
        version="2",
        feature_definitions={"elo_a": {}, "elo_b": {}, "h2h_a_wins": {}},
    )
    session.add_all([fs_old, fs_new])

    old_mf = MatchFeatures(
        match_id=1,
        feature_set_id=1,
        features={"elo_a": 1, "elo_b": 2},
        computed_at=datetime(2026, 4, 20),
    )
    new_mf = MatchFeatures(
        match_id=1,
        feature_set_id=2,
        features={"elo_a": 3, "elo_b": 4, "h2h_a_wins": 1},
        # Newer computed_at than old_mf
        computed_at=datetime(2026, 4, 22),
    )
    session.add_all([old_mf, new_mf])
    session.commit()

    # Explanation points to the OLDER feature set — router must honour that
    # rather than picking the newest snapshot.
    explanation = {"feature_set_name": "baseline_v1"}
    selected = select_match_features_for_view(session, 1, explanation)
    assert selected is not None
    assert selected.feature_set.name == "baseline_v1"


def test_select_match_features_falls_back_to_newest_without_explanation() -> None:
    session = _build_session()
    _seed_minimal_match(session)

    fs_old = FeatureSet(
        id=1,
        name="baseline_v1",
        version="1",
        feature_definitions={"elo_a": {}, "elo_b": {}},
    )
    fs_new = FeatureSet(
        id=2,
        name="baseline_v2",
        version="2",
        feature_definitions={"elo_a": {}, "elo_b": {}},
    )
    session.add_all([fs_old, fs_new])
    old_mf = MatchFeatures(
        match_id=1,
        feature_set_id=1,
        features={"elo_a": 1, "elo_b": 2},
        computed_at=datetime(2026, 4, 20),
    )
    new_mf = MatchFeatures(
        match_id=1,
        feature_set_id=2,
        features={"elo_a": 3, "elo_b": 4},
        computed_at=datetime(2026, 4, 22),
    )
    session.add_all([old_mf, new_mf])
    session.commit()

    selected = select_match_features_for_view(session, 1, None)
    assert selected is not None
    assert selected.feature_set.name == "baseline_v2"


def test_select_match_features_returns_none_for_no_snapshots() -> None:
    session = _build_session()
    _seed_minimal_match(session)
    assert select_match_features_for_view(session, 1, None) is None


def test_select_match_features_falls_back_when_feature_set_missing() -> None:
    session = _build_session()
    _seed_minimal_match(session)

    fs = FeatureSet(
        id=1,
        name="baseline_v2",
        version="2",
        feature_definitions={"elo_a": {}},
    )
    mf = MatchFeatures(
        match_id=1,
        feature_set_id=1,
        features={"elo_a": 3.0},
        computed_at=datetime(2026, 4, 22),
    )
    session.add_all([fs, mf])
    session.commit()

    # Explanation references a feature set name that isn't in the DB
    explanation = {"feature_set_name": "missing_fs"}
    selected = select_match_features_for_view(session, 1, explanation)
    assert selected is not None
    assert selected.feature_set.name == "baseline_v2"
