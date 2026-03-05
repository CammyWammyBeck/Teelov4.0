from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import re
from typing import Any, Optional

from teelo.db.models import Match


_SET_SCORE_RE = re.compile(r"^(\d+)-(\d+)(\(\d+\))?$")


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


def flip_score_for_display(score: Optional[str]) -> Optional[str]:
    if not score:
        return score
    parts = score.split()
    flipped_parts: list[str] = []
    for part in parts:
        match = _SET_SCORE_RE.match(part)
        if match:
            suffix = match.group(3) or ""
            flipped_parts.append(f"{match.group(2)}-{match.group(1)}{suffix}")
        else:
            flipped_parts.append(part)
    return " ".join(flipped_parts)


def serialize_match(match: Match) -> dict:
    te = match.tournament_edition
    tournament = te.tournament if te else None
    surface = (te.surface if te and te.surface else None) or (tournament.surface if tournament else None)
    pa = match.player_a
    pb = match.player_b
    display_date = match.match_date or match.scheduled_date
    player_a_payload = {
        "id": pa.id if pa else match.player_a_id,
        "name": pa.canonical_name if pa else "Unknown",
        "seed": match.player_a_seed,
        "elo_pre": round_elo(match.elo_pre_player_a),
        "elo_change": round_elo(match.elo_post_player_a - match.elo_pre_player_a) if match.elo_post_player_a is not None and match.elo_pre_player_a is not None else None,
    }
    player_b_payload = {
        "id": pb.id if pb else match.player_b_id,
        "name": pb.canonical_name if pb else "Unknown",
        "seed": match.player_b_seed,
        "elo_pre": round_elo(match.elo_pre_player_b),
        "elo_change": round_elo(match.elo_post_player_b - match.elo_pre_player_b) if match.elo_post_player_b is not None and match.elo_pre_player_b is not None else None,
    }
    swap_key = f"{match.id}:{match.temporal_order or 0}"
    swap_display_sides = (hashlib.blake2s(swap_key.encode("utf-8"), digest_size=1).digest()[0] & 1) == 1
    display_score = match.score
    if swap_display_sides:
        player_a_payload, player_b_payload = player_b_payload, player_a_payload
        display_score = flip_score_for_display(display_score)
    return {
        "id": match.id,
        "tour": tournament.tour if tournament else None,
        "gender": tournament.gender if tournament else None,
        "tournament_name": tournament.name if tournament else None,
        "tournament_level": tournament.level if tournament else None,
        "surface": surface,
        "round": match.round,
        "player_a": player_a_payload,
        "player_b": player_b_payload,
        "score": display_score,
        "winner_id": match.winner_id,
        "status": match.status,
        "match_date": display_date.isoformat() if display_date else None,
        "match_date_display": display_date.strftime("%d %b %Y") if display_date else None,
        "year": display_date.year if display_date else (te.year if te else None),
    }
