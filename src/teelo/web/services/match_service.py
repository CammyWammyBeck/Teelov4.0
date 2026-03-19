from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import case, func

from teelo.db.models import Match


_SLUG_NON_ALNUM_RE = re.compile(r"[^a-z0-9\u00C0-\u024F]+")
_SLUG_MULTI_HYPHEN_RE = re.compile(r"-+")


def upcoming_sort_expressions():
    """Standard sort order for upcoming matches: scheduled first, then by date/time."""
    has_schedule = case(
        (Match.scheduled_date.isnot(None), 0),
        else_=1,
    )
    return [
        has_schedule.asc(),
        Match.scheduled_date.asc().nullslast(),
        Match.scheduled_datetime.asc().nullslast(),
        Match.id.asc(),
    ]


def completed_sort_expressions():
    """Standard sort order for completed matches: most recent first."""
    return [
        func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
        Match.temporal_order.desc().nullslast(),
        Match.id.desc(),
    ]


def round_elo(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def resolve_date_preset(preset: str) -> tuple[Optional[date], Optional[date]]:
    today = date.today()
    if preset == "7d":
        return today - timedelta(days=7), today
    if preset == "30d":
        return today - timedelta(days=30), today
    if preset == "90d":
        return today - timedelta(days=90), today
    if preset == "ytd":
        return date(today.year, 1, 1), today
    if preset.isdigit() and len(preset) == 4:
        year = int(preset)
        return date(year, 1, 1), date(year, 12, 31)
    return None, None


def slugify_name(name: Optional[str]) -> str:
    if not name:
        return ""
    slug = _SLUG_NON_ALNUM_RE.sub("-", str(name).lower())
    slug = _SLUG_MULTI_HYPHEN_RE.sub("-", slug)
    return slug.strip("-")


def serialize_match(match: Match) -> dict:
    te = match.tournament_edition
    tournament = te.tournament if te else None
    surface = (
        (te.surface if te and te.surface else None)
        or (tournament.surface if tournament else None)
    )
    pa = match.player_a
    pb = match.player_b
    exact_datetime = match.match_datetime or match.scheduled_datetime
    # Convert naive local datetime to UTC using tournament timezone
    tz_name = tournament.timezone if tournament else None
    if exact_datetime and tz_name and exact_datetime.tzinfo is None:
        try:
            local_tz = ZoneInfo(tz_name)
            exact_datetime = exact_datetime.replace(tzinfo=local_tz).astimezone(
                timezone.utc
            )
        except (KeyError, ValueError):
            pass  # unknown timezone — keep naive datetime
    elif exact_datetime and exact_datetime.tzinfo is None:
        # No timezone available — cannot produce a reliable UTC value
        exact_datetime = None
    display_date = (
        (match.match_datetime or match.scheduled_datetime or datetime.min).date()
        if (match.match_datetime or match.scheduled_datetime)
        else (match.match_date or match.scheduled_date)
    )
    # If we have a date but no exact datetime, synthesize one at 23:59 UTC
    # so the client can still convert the date to local timezone
    if exact_datetime:
        display_datetime_utc = exact_datetime
        has_exact_time = True
    elif display_date:
        display_datetime_utc = datetime.combine(
            display_date, time(23, 59), tzinfo=timezone.utc
        )
        has_exact_time = False
    else:
        display_datetime_utc = None
        has_exact_time = False
    player_a_payload = {
        "id": pa.id if pa else match.player_a_id,
        "name": pa.canonical_name if pa else "Unknown",
        "player_url": (
            f"/players/{pa.id}/{slugify_name(pa.canonical_name)}"
            if pa and pa.id is not None
            else (
                f"/players/{match.player_a_id}"
                if match.player_a_id is not None
                else None
            )
        ),
        "seed": match.player_a_seed,
        "elo_pre": round_elo(match.elo_pre_player_a),
        "elo_change": (
            round_elo(match.elo_post_player_a - match.elo_pre_player_a)
            if match.elo_post_player_a is not None
            and match.elo_pre_player_a is not None
            else None
        ),
    }
    player_b_payload = {
        "id": pb.id if pb else match.player_b_id,
        "name": pb.canonical_name if pb else "Unknown",
        "player_url": (
            f"/players/{pb.id}/{slugify_name(pb.canonical_name)}"
            if pb and pb.id is not None
            else (
                f"/players/{match.player_b_id}"
                if match.player_b_id is not None
                else None
            )
        ),
        "seed": match.player_b_seed,
        "elo_pre": round_elo(match.elo_pre_player_b),
        "elo_change": (
            round_elo(match.elo_post_player_b - match.elo_pre_player_b)
            if match.elo_post_player_b is not None
            and match.elo_pre_player_b is not None
            else None
        ),
    }
    prediction_a_val = match.prediction_a
    if prediction_a_val is not None:
        prediction_a_val = float(prediction_a_val)
    return {
        "id": match.id,
        "tour": tournament.tour if tournament else None,
        "gender": tournament.gender if tournament else None,
        "tournament_name": tournament.name if tournament else None,
        "tournament_code": tournament.tournament_code if tournament else None,
        "tournament_url": (
            f"/tournaments/{tournament.tour.lower()}"
            f"/{tournament.tournament_code}/{te.year}"
            if tournament and tournament.tour and te
            else None
        ),
        "tournament_level": tournament.level if tournament else None,
        "surface": surface,
        "round": match.round,
        "player_a": player_a_payload,
        "player_b": player_b_payload,
        "score": match.score,
        "winner_id": match.winner_id,
        "status": match.status,
        "match_date": (
            display_date.isoformat() if display_date else None
        ),
        "match_date_display": (
            display_date.strftime("%d %b %Y") if display_date else None
        ),
        "match_datetime_utc": (
            display_datetime_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            if display_datetime_utc
            else None
        ),
        "has_exact_time": has_exact_time,
        "year": (
            display_date.year
            if display_date
            else (te.year if te else None)
        ),
        "prediction_a": prediction_a_val,
        "match_url": f"/matches/{match.id}",
    }
