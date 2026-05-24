from datetime import date, datetime

from teelo.db.models import estimate_match_date_from_round


def test_estimate_match_date_accepts_mixed_date_and_datetime_bounds():
    assert estimate_match_date_from_round(
        "F", date(2026, 4, 21), datetime(2026, 5, 3)
    ) == date(2026, 5, 3)
    assert estimate_match_date_from_round(
        "F", datetime(2026, 4, 21), date(2026, 5, 3)
    ) == date(2026, 5, 3)
