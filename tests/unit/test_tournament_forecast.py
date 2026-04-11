from __future__ import annotations

from datetime import date

import pytest

from teelo.db.models import (
    Match,
    Player,
    Tournament,
    TournamentEdition,
    TournamentForecastNode,
    TournamentForecastRun,
)
from teelo.services.tournament_forecast import (
    _json_sanitize,
    compute_probabilities,
    is_draw_forecast_ready,
    is_forecast_eligible_tournament,
)


def _make_player(session, pid: int, name: str) -> Player:
    p = Player(
        id=pid,
        canonical_name=name,
        gender="men",
    )
    session.add(p)
    return p


def test_forecast_eligibility_filter(db_session):
    t1 = Tournament(
        tournament_code="TEST",
        name="Test",
        tour="ATP",
        gender="men",
        level="ATP 250",
    )
    t2 = Tournament(
        tournament_code="TEST2",
        name="Test2",
        tour="Challenger",
        gender="men",
        level="Challenger",
    )
    assert is_forecast_eligible_tournament(t1) is True
    assert is_forecast_eligible_tournament(t2) is False


def test_features_json_sanitizer_converts_dates():
    payload = {
        "match_date": date(2026, 4, 3),
        "nested": {"dates": [date(2026, 4, 4)]},
        "matches": [("x", date(2026, 4, 5))],
    }
    out = _json_sanitize(payload)
    assert out["match_date"] == "2026-04-03"
    assert out["nested"]["dates"][0] == "2026-04-04"
    assert out["matches"][0][1] == "2026-04-05"


def test_draw_forecast_ready_no_byes_requires_all_entry_matches(db_session):
    t = Tournament(
        tournament_code="TESTREADY",
        name="Test Ready",
        tour="ATP",
        gender="men",
        level="ATP 250",
    )
    db_session.add(t)
    db_session.flush()

    # 32-player draw: entry round R32, second round R16.
    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=32)
    db_session.add(edition)
    db_session.flush()

    # Create 32 players.
    for pid in range(1, 33):
        _make_player(db_session, pid, f"P{pid}")
    db_session.flush()

    # Populate all 16 R32 matches with known players (no results required).
    matches = []
    pid = 1
    for pos in range(1, 17):
        matches.append(
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R32",
                draw_position=pos,
                player_a_id=pid,
                player_b_id=pid + 1,
                status="upcoming",
            )
        )
        pid += 2
    db_session.add_all(matches)
    db_session.flush()

    assert is_draw_forecast_ready(db_session, edition) is True


def test_draw_forecast_ready_with_byes_materialised_in_second_round(db_session):
    t = Tournament(
        tournament_code="TESTREADYB",
        name="Test Ready Byes",
        tour="ATP",
        gender="men",
        level="ATP 250",
    )
    db_session.add(t)
    db_session.flush()

    # Use 16-player draw shape for easy maths: entry round R16, second round QF.
    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=16)
    db_session.add(edition)
    db_session.flush()

    for pid in range(1, 17):
        _make_player(db_session, pid, f"P{pid}")
    db_session.flush()

    # Represent byes as synthetic completed entry-round matches (player vs BYE).
    bye = Player(canonical_name="BYE", gender="men")
    db_session.add(bye)
    db_session.flush()

    # Top feeder slots (R16 positions 1,3,5,7) are byes: deterministic winners.
    bye_matches = [
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="R16",
            draw_position=pos,
            player_a_id=pos,  # arbitrary seed
            player_b_id=bye.id,
            status="completed",
            winner_id=pos,
            score="BYE",
        )
        for pos in [1, 3, 5, 7]
    ]
    db_session.add_all(bye_matches)

    # Bottom feeder matches (R16 positions 2,4,6,8) are known but not completed.
    bottom_r16 = [
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="R16",
            draw_position=2,
            player_a_id=9,
            player_b_id=10,
            status="upcoming",
        ),
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="R16",
            draw_position=4,
            player_a_id=11,
            player_b_id=12,
            status="upcoming",
        ),
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="R16",
            draw_position=6,
            player_a_id=13,
            player_b_id=14,
            status="upcoming",
        ),
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="R16",
            draw_position=8,
            player_a_id=15,
            player_b_id=16,
            status="upcoming",
        ),
    ]

    db_session.add_all(bottom_r16)
    db_session.flush()

    assert is_draw_forecast_ready(db_session, edition) is True


def test_draw_forecast_ready_prepopulated_second_round_match_counts(db_session):
    t = Tournament(
        tournament_code="TESTREADYC",
        name="Test Ready Prepop",
        tour="ATP",
        gender="men",
        level="ATP 250",
    )
    db_session.add(t)
    db_session.flush()

    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=16)
    db_session.add(edition)
    db_session.flush()

    for pid in range(1, 17):
        _make_player(db_session, pid, f"P{pid}")
    db_session.flush()

    # QF#1 is fully prepopulated (bye-vs-bye / auto-advance style).
    db_session.add(
        Match(
            source="atp",
            tournament_edition_id=edition.id,
            round="QF",
            draw_position=1,
            player_a_id=1,
            player_b_id=2,
            status="upcoming",
        )
    )

    bye = Player(canonical_name="BYE", gender="men")
    db_session.add(bye)
    db_session.flush()

    # Remaining QF slots are made resolvable via entry-round bye matches + known bottom feeders.
    db_session.add_all(
        [
            # Bye matches at top feeder slots for QF#2..#4: R16#3, #5, #7
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=3,
                player_a_id=3,
                player_b_id=bye.id,
                status="completed",
                winner_id=3,
                score="BYE",
            ),
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=5,
                player_a_id=4,
                player_b_id=bye.id,
                status="completed",
                winner_id=4,
                score="BYE",
            ),
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=7,
                player_a_id=5,
                player_b_id=bye.id,
                status="completed",
                winner_id=5,
                score="BYE",
            ),
            # Bottom feeders for QF#2..#4 (R16#4, #6, #8)
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=4,
                player_a_id=9,
                player_b_id=10,
                status="upcoming",
            ),
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=6,
                player_a_id=11,
                player_b_id=12,
                status="upcoming",
            ),
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R16",
                draw_position=8,
                player_a_id=13,
                player_b_id=14,
                status="upcoming",
            ),
        ]
    )

    db_session.flush()

    assert is_draw_forecast_ready(db_session, edition) is True


def test_compute_probabilities_4_player_draw(db_session):
    # Tournament + edition
    t = Tournament(
        tournament_code="TEST",
        name="Test",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
    )
    db_session.add(t)
    db_session.flush()

    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=4)
    db_session.add(edition)
    db_session.flush()

    _make_player(db_session, 1, "A")
    _make_player(db_session, 2, "B")
    _make_player(db_session, 3, "C")
    _make_player(db_session, 4, "D")
    db_session.flush()

    # Real SF matches
    m1 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        status="upcoming",
        prediction_a=0.6,
        prediction_model_version="v",
    )
    m2 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        status="upcoming",
        prediction_a=0.7,
        prediction_model_version="v",
    )
    db_session.add_all([m1, m2])
    db_session.flush()

    run = TournamentForecastRun(
        tournament_edition_id=edition.id,
        status="ready",
        build_reason="initial",
        structure_signature="s",
        state_signature="st",
        feature_set_name="fs",
        model_version="v",
        is_active=True,
    )
    db_session.add(run)
    db_session.flush()

    # Actual SF nodes
    n_sf1 = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        source_match_id=m1.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.6,
    )
    n_sf2 = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        source_match_id=m2.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.7,
    )

    # Persist SF nodes first so we can reference them as parents for path-specific final nodes.
    db_session.add_all([n_sf1, n_sf2])
    db_session.flush()

    # Scenario final nodes for all cross-product matchups.
    # P(A wins) values chosen arbitrarily but must be in [0,1].
    finals = [
        (1, 3, 0.55),
        (1, 4, 0.65),
        (2, 3, 0.45),
        (2, 4, 0.50),
    ]
    n_f = [
        TournamentForecastNode(
            forecast_run_id=run.id,
            round="F",
            draw_position=1,
            player_a_id=a,
            player_b_id=b,
            source_match_id=None,
            node_type="scenario",
            generation_depth=1,
            feature_set_name="fs",
            prediction_model_version="v",
            prediction_a=pa,
            left_parent_node_id=n_sf1.id,
            right_parent_node_id=n_sf2.id,
        )
        for a, b, pa in finals
    ]

    db_session.add_all(n_f)
    db_session.commit()

    payload = compute_probabilities(db_session, edition_id=edition.id)
    assert payload["has_forecast"] is True

    # Title probs should sum to ~1
    title_sum = sum(p.get("win_title", 0.0) for p in payload["players"])
    assert title_sum == pytest.approx(1.0, abs=1e-6)

    # All players should have reach_f defined (prob of making final)
    for p in payload["players"]:
        assert "reach_f" in p
        assert 0.0 <= p["reach_f"] <= 1.0



def test_completed_early_round_collapses_eliminated_mass(db_session):
    """If a real early-round match is completed, eliminated players should carry zero future mass."""

    t = Tournament(
        tournament_code="TESTCOLLAPSE",
        name="Test Collapse",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
    )
    db_session.add(t)
    db_session.flush()

    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=4)
    db_session.add(edition)
    db_session.flush()

    for pid, name in [(1, "A"), (2, "B"), (3, "C"), (4, "D")]:
        _make_player(db_session, pid, name)
    db_session.flush()

    # SF1 completed: A beats B.
    m_sf1 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        status="completed",
        winner_id=1,
        score_structured=[{"a": 6, "b": 4}, {"a": 6, "b": 4}],
        prediction_a=0.6,
        prediction_model_version="v",
    )
    # SF2 upcoming.
    m_sf2 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        status="upcoming",
        prediction_a=0.7,
        prediction_model_version="v",
    )
    db_session.add_all([m_sf1, m_sf2])
    db_session.flush()

    run = TournamentForecastRun(
        tournament_edition_id=edition.id,
        status="ready",
        build_reason="initial",
        structure_signature="s",
        state_signature="st",
        feature_set_name="fs",
        model_version="v",
        is_active=True,
    )
    db_session.add(run)
    db_session.flush()

    n_sf1 = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        source_match_id=m_sf1.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.6,
    )
    n_sf2 = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        source_match_id=m_sf2.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.7,
    )
    db_session.add_all([n_sf1, n_sf2])
    db_session.flush()

    # Final scenario nodes include impossible matchups involving eliminated player B (2).
    finals = [
        (1, 3, 0.55),
        (1, 4, 0.65),
        (2, 3, 0.45),
        (2, 4, 0.50),
    ]
    db_session.add_all(
        [
            TournamentForecastNode(
                forecast_run_id=run.id,
                round="F",
                draw_position=1,
                player_a_id=a,
                player_b_id=b,
                source_match_id=None,
                node_type="scenario",
                generation_depth=1,
                feature_set_name="fs",
                prediction_model_version="v",
                prediction_a=pa,
                left_parent_node_id=n_sf1.id,
                right_parent_node_id=n_sf2.id,
            )
            for a, b, pa in finals
        ]
    )
    db_session.commit()

    payload = compute_probabilities(db_session, edition_id=edition.id)

    p2 = next((p for p in payload["players"] if p["player_id"] == 2), None)
    if p2 is not None:
        assert p2.get("reach_f", 0.0) == 0.0
        assert p2.get("win_title", 0.0) == 0.0
    else:
        # Eliminated players may not be included in the payload at all.
        assert True


def test_completed_two_rounds_title_probs_start_from_current_state(db_session):
    """If QFs are completed, only the remaining SF entrants should have any title mass."""

    t = Tournament(
        tournament_code="TESTSTATE",
        name="Test State",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
    )
    db_session.add(t)
    db_session.flush()

    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=8)
    db_session.add(edition)
    db_session.flush()

    for pid in range(1, 9):
        _make_player(db_session, pid, f"P{pid}")
    db_session.flush()

    # All QFs completed: winners 1,3,5,7.
    qfs = []
    for pos, (a, b, w) in enumerate([(1, 2, 1), (3, 4, 3), (5, 6, 5), (7, 8, 7)], start=1):
        qfs.append(
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="QF",
                draw_position=pos,
                player_a_id=a,
                player_b_id=b,
                status="completed",
                winner_id=w,
                score_structured=[{"a": 6, "b": 4}, {"a": 6, "b": 4}],
                prediction_a=0.5,
                prediction_model_version="v",
            )
        )
    db_session.add_all(qfs)
    db_session.flush()

    run = TournamentForecastRun(
        tournament_edition_id=edition.id,
        status="ready",
        build_reason="initial",
        structure_signature="s",
        state_signature="st",
        feature_set_name="fs",
        model_version="v",
        is_active=True,
    )
    db_session.add(run)
    db_session.flush()

    # Actual QF nodes.
    qf_nodes = []
    for pos, m in enumerate(qfs, start=1):
        qf_nodes.append(
            TournamentForecastNode(
                forecast_run_id=run.id,
                round="QF",
                draw_position=pos,
                player_a_id=m.player_a_id,
                player_b_id=m.player_b_id,
                source_match_id=m.id,
                node_type="actual",
                generation_depth=0,
                feature_set_name="fs",
                prediction_model_version="v",
                prediction_a=0.5,
            )
        )
    db_session.add_all(qf_nodes)
    db_session.flush()

    # SF scenario nodes (each slot has two potential opponents from its feeders).
    # SF1 feeders: QF1 winner vs QF2 winner. Only (1 vs 3) occurs after completions.
    sf1_nodes = [
        TournamentForecastNode(
            forecast_run_id=run.id,
            round="SF",
            draw_position=1,
            player_a_id=a,
            player_b_id=b,
            source_match_id=None,
            node_type="scenario",
            generation_depth=1,
            feature_set_name="fs",
            prediction_model_version="v",
            prediction_a=0.6,
            left_parent_node_id=qf_nodes[0].id,
            right_parent_node_id=qf_nodes[1].id,
        )
        for a, b in [(1, 3), (2, 3), (1, 4), (2, 4)]
    ]
    # SF2 feeders: QF3 winner vs QF4 winner. Only (5 vs 7) occurs.
    sf2_nodes = [
        TournamentForecastNode(
            forecast_run_id=run.id,
            round="SF",
            draw_position=2,
            player_a_id=a,
            player_b_id=b,
            source_match_id=None,
            node_type="scenario",
            generation_depth=1,
            feature_set_name="fs",
            prediction_model_version="v",
            prediction_a=0.55,
            left_parent_node_id=qf_nodes[2].id,
            right_parent_node_id=qf_nodes[3].id,
        )
        for a, b in [(5, 7), (6, 7), (5, 8), (6, 8)]
    ]
    db_session.add_all(sf1_nodes + sf2_nodes)
    db_session.flush()

    # Final scenario nodes for cross-product of SF winners (only paths involving 1/3 and 5/7 survive).
    db_session.add_all(
        [
            TournamentForecastNode(
                forecast_run_id=run.id,
                round="F",
                draw_position=1,
                player_a_id=a,
                player_b_id=b,
                source_match_id=None,
                node_type="scenario",
                generation_depth=2,
                feature_set_name="fs",
                prediction_model_version="v",
                prediction_a=0.5,
                left_parent_node_id=sf1_nodes[0].id,
                right_parent_node_id=sf2_nodes[0].id,
            )
            for a in [1, 3, 2, 4]
            for b in [5, 7, 6, 8]
            if a != b
        ]
    )

    db_session.commit()

    payload = compute_probabilities(db_session, edition_id=edition.id)

    # Eliminated QF losers have zero title probability (or are omitted entirely).
    for pid in [2, 4, 6, 8]:
        pl = next((p for p in payload["players"] if p["player_id"] == pid), None)
        if pl is not None:
            assert pl.get("win_title", 0.0) == 0.0

    # QF winners reach SF with probability 1.0.
    for pid in [1, 3, 5, 7]:
        pl = next(p for p in payload["players"] if p["player_id"] == pid)
        assert pl.get("reach_sf", 0.0) == pytest.approx(1.0, abs=1e-9)


def test_real_known_matchup_node_overrides_scenario_nodes(db_session):
    """If a slot has an actual node, it should be used even if scenario nodes exist."""

    t = Tournament(
        tournament_code="TESTOVERRIDE",
        name="Test Override",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
    )
    db_session.add(t)
    db_session.flush()

    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=4)
    db_session.add(edition)
    db_session.flush()

    for pid, name in [(1, "A"), (2, "B"), (3, "C"), (4, "D")]:
        _make_player(db_session, pid, name)
    db_session.flush()

    m_sf1 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        status="upcoming",
        prediction_a=0.8,
        prediction_model_version="v",
    )
    m_sf2 = Match(
        source="atp",
        tournament_edition_id=edition.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        status="upcoming",
        prediction_a=0.5,
        prediction_model_version="v",
    )
    db_session.add_all([m_sf1, m_sf2])
    db_session.flush()

    run = TournamentForecastRun(
        tournament_edition_id=edition.id,
        status="ready",
        build_reason="initial",
        structure_signature="s",
        state_signature="st",
        feature_set_name="fs",
        model_version="v",
        is_active=True,
    )
    db_session.add(run)
    db_session.flush()

    n_sf1_actual = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        source_match_id=m_sf1.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.8,
    )
    # Conflicting scenario node for same slot (should be ignored).
    n_sf1_scenario = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=1,
        player_a_id=1,
        player_b_id=2,
        source_match_id=None,
        node_type="scenario",
        generation_depth=1,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.1,
        left_parent_node_id=999,
        right_parent_node_id=1000,
    )
    n_sf2 = TournamentForecastNode(
        forecast_run_id=run.id,
        round="SF",
        draw_position=2,
        player_a_id=3,
        player_b_id=4,
        source_match_id=m_sf2.id,
        node_type="actual",
        generation_depth=0,
        feature_set_name="fs",
        prediction_model_version="v",
        prediction_a=0.5,
    )

    db_session.add_all([n_sf1_actual, n_sf1_scenario, n_sf2])
    db_session.flush()

    # Finals include only the plausible cross-product for brevity.
    db_session.add(
        TournamentForecastNode(
            forecast_run_id=run.id,
            round="F",
            draw_position=1,
            player_a_id=1,
            player_b_id=3,
            source_match_id=None,
            node_type="scenario",
            generation_depth=2,
            feature_set_name="fs",
            prediction_model_version="v",
            prediction_a=0.5,
            left_parent_node_id=n_sf1_actual.id,
            right_parent_node_id=n_sf2.id,
        )
    )

    db_session.commit()

    payload = compute_probabilities(db_session, edition_id=edition.id)
    p1 = next(p for p in payload["players"] if p["player_id"] == 1)
    assert p1.get("reach_f", 0.0) == pytest.approx(0.8, abs=1e-9)


def test_draw_forecast_ready_prefers_db_rounds_over_draw_size(db_session):
    """Regression: some editions have stale/mis-set draw_size after migrations.

    Readiness should be based on the earliest round present in the DB, not the draw_size mapping.
    """

    t = Tournament(
        tournament_code="TESTREADYSTALE",
        name="Test Ready Stale DrawSize",
        tour="ATP",
        gender="men",
        level="ATP 250",
    )
    db_session.add(t)
    db_session.flush()

    # Deliberately wrong draw_size (96 would imply R128 entry), but DB will contain an R64 draw.
    edition = TournamentEdition(tournament_id=t.id, year=2026, draw_size=96)
    db_session.add(edition)
    db_session.flush()

    # Create 64 players.
    for pid in range(1, 65):
        _make_player(db_session, pid, f"P{pid}")
    db_session.flush()

    # Populate all 32 R64 matches with known players.
    matches = []
    pid = 1
    for pos in range(1, 33):
        matches.append(
            Match(
                source="atp",
                tournament_edition_id=edition.id,
                round="R64",
                draw_position=pos,
                player_a_id=pid,
                player_b_id=pid + 1,
                status="upcoming",
            )
        )
        pid += 2
    db_session.add_all(matches)
    db_session.flush()

    assert is_draw_forecast_ready(db_session, edition) is True
