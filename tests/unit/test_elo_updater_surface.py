from datetime import date

from teelo.elo.pipeline import EloParams
from teelo.elo.updater import EloUpdater, _MatchRow, resolve_surface_bucket


def test_resolve_surface_bucket_prefers_indoor() -> None:
    assert resolve_surface_bucket("Clay", "Clay", "Indoor") == "Indoor"
    assert resolve_surface_bucket(None, "Hard", "indoor") == "Indoor"


def test_resolve_surface_bucket_normalizes_values() -> None:
    assert resolve_surface_bucket("Hard", None, None) == "Hard"
    assert resolve_surface_bucket("clay", None, None) == "Clay"
    assert resolve_surface_bucket(None, "grass", None) == "Grass"
    assert resolve_surface_bucket(None, None, None) == "Hard"


def test_process_matches_tracks_surface_states_independently() -> None:
    updater = EloUpdater(params=EloParams(), params_version="test-v1")
    matches = [
        _MatchRow(
            id=1,
            player_a_id=101,
            player_b_id=202,
            winner_id=101,
            temporal_order=2026010100000010,
            match_date=date(2026, 1, 1),
            score_structured=None,
            level_code="A",
            surface="Hard",
        ),
        _MatchRow(
            id=2,
            player_a_id=101,
            player_b_id=202,
            winner_id=202,
            temporal_order=2026010200000010,
            match_date=date(2026, 1, 2),
            score_structured=None,
            level_code="A",
            surface="Clay",
        ),
    ]

    overall_states = {}
    surface_states = {}
    _, _, surface_snapshots, touched_surface_keys = updater._process_matches(
        matches,
        overall_states,
        surface_states,
    )

    assert len(surface_snapshots) == 4
    assert touched_surface_keys == {
        (101, "Hard"),
        (202, "Hard"),
        (101, "Clay"),
        (202, "Clay"),
    }
    assert surface_states[(101, "Hard")].match_count == 1
    assert surface_states[(202, "Hard")].match_count == 1
    assert surface_states[(101, "Clay")].match_count == 1
    assert surface_states[(202, "Clay")].match_count == 1
