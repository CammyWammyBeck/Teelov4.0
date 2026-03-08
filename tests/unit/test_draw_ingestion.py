from teelo.db.models import Tournament, TournamentEdition
from teelo.services.draw_ingestion import _infer_draw_source


def test_infer_draw_source_prefers_edition_ids():
    tournament = Tournament(
        tournament_code="INDIAN-WELLS",
        name="Indian Wells",
        tour="ATP",
        gender="men",
        level="Masters 1000",
    )
    edition = TournamentEdition(
        year=2026,
        tournament=tournament,
        wta_edition_id="1234",
    )

    assert _infer_draw_source(edition) == "wta"


def test_infer_draw_source_uses_tournament_tour():
    tournament = Tournament(
        tournament_code="INDIAN-WELLS",
        name="Indian Wells",
        tour="WTA",
        gender="women",
        level="WTA 1000",
    )
    edition = TournamentEdition(
        year=2026,
        tournament=tournament,
    )

    assert _infer_draw_source(edition) == "wta"


def test_infer_draw_source_maps_wta_125_to_wta():
    tournament = Tournament(
        tournament_code="AUSTIN",
        name="Austin",
        tour="WTA 125",
        gender="women",
        level="WTA 125",
    )
    edition = TournamentEdition(
        year=2026,
        tournament=tournament,
    )

    assert _infer_draw_source(edition) == "wta"


def test_infer_draw_source_falls_back_to_gender():
    tournament = Tournament(
        tournament_code="EXHIBITION",
        name="Exhibition",
        tour="Unknown",
        gender="women",
        level="Exhibition",
    )
    edition = TournamentEdition(
        year=2026,
        tournament=tournament,
    )

    assert _infer_draw_source(edition) == "wta"
