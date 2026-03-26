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
