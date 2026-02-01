"""
Margin of victory calculations for ELO K-factor adjustment.

In standard ELO, a 6-0 6-0 blowout and a 7-6 7-6 squeaker produce the same
rating change. This module adjusts the K-factor based on how dominant the win
was, so blowouts move ratings more and close matches move them less.

The margin multiplier is applied to K: effective_K = K * margin_multiplier

Multiplier ranges:
- ~0.8 for very close matches (tiebreaks, tight sets)
- ~1.0 for "normal" wins
- ~1.5 for dominant wins (bagels, breadsticks)

The formula:
    multiplier = margin_base + (games_margin / max_possible_margin) * margin_scale

Where games_margin is the normalized dominance metric from the score.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from teelo.elo.constants import MARGIN_DEFAULTS


@dataclass
class MarginResult:
    """
    Result of margin-of-victory calculation.

    Attributes:
        multiplier: K-factor multiplier (typically 0.7 - 1.5)
        games_won_winner: Total games won by the winner
        games_won_loser: Total games won by the loser
        sets_won_winner: Sets won by the winner
        sets_won_loser: Sets won by the loser
        dominance_ratio: Normalized dominance metric (0.0 = closest possible, 1.0 = most dominant)
    """
    multiplier: Decimal
    games_won_winner: int
    games_won_loser: int
    sets_won_winner: int
    sets_won_loser: int
    dominance_ratio: Decimal


def calculate_margin_multiplier(
    score_structured: list[dict],
    winner: str,
    margin_base: Optional[float] = None,
    margin_scale: Optional[float] = None,
) -> MarginResult:
    """
    Calculate a K-factor multiplier based on the match score.

    Analyzes the score to determine how dominant the win was, then
    produces a multiplier to scale the ELO K-factor. Close matches
    get multipliers below 1.0 (smaller rating changes), dominant wins
    get multipliers above 1.0 (bigger rating changes).

    Args:
        score_structured: Parsed score in structured format from the DB.
            Each set is a dict: {"a": 6, "b": 4, "tb_a": 7, "tb_b": 5}
        winner: 'A' or 'B' indicating who won
        margin_base: Base value for multiplier (default from constants)
        margin_scale: Scale factor for dominance (default from constants)

    Returns:
        MarginResult with the multiplier and score analysis

    Examples:
        # Dominant win: 6-0 6-0 → multiplier ~1.5
        result = calculate_margin_multiplier(
            [{"a": 6, "b": 0}, {"a": 6, "b": 0}], winner="A"
        )

        # Close match: 7-6 7-6 → multiplier ~0.8
        result = calculate_margin_multiplier(
            [{"a": 7, "b": 6, "tb_a": 7, "tb_b": 5},
             {"a": 7, "b": 6, "tb_a": 7, "tb_b": 3}], winner="A"
        )
    """
    base = Decimal(str(margin_base if margin_base is not None else MARGIN_DEFAULTS["margin_base"]))
    scale = Decimal(str(margin_scale if margin_scale is not None else MARGIN_DEFAULTS["margin_scale"]))

    if not score_structured:
        # No score data — return neutral multiplier
        return MarginResult(
            multiplier=Decimal("1.0"),
            games_won_winner=0,
            games_won_loser=0,
            sets_won_winner=0,
            sets_won_loser=0,
            dominance_ratio=Decimal("0.5"),
        )

    # Count games and sets for each player
    total_games_a = 0
    total_games_b = 0
    sets_a = 0
    sets_b = 0

    for set_data in score_structured:
        ga = set_data.get("a", 0)
        gb = set_data.get("b", 0)
        total_games_a += ga
        total_games_b += gb

        if ga > gb:
            sets_a += 1
        elif gb > ga:
            sets_b += 1

    # Assign winner/loser games
    if winner == "A":
        games_won_winner = total_games_a
        games_won_loser = total_games_b
        sets_won_winner = sets_a
        sets_won_loser = sets_b
    else:
        games_won_winner = total_games_b
        games_won_loser = total_games_a
        sets_won_winner = sets_b
        sets_won_loser = sets_a

    # Calculate dominance ratio (0.0 = closest possible, 1.0 = most dominant)
    # Based on game differential relative to total games played
    total_games = games_won_winner + games_won_loser

    if total_games == 0:
        dominance_ratio = Decimal("0.5")
    else:
        game_diff = games_won_winner - games_won_loser
        # Maximum possible differential in a match with this many sets:
        # Best of 3: 2 sets, max diff = 12 (6-0 6-0)
        # Best of 5: 3 sets, max diff = 18 (6-0 6-0 6-0)
        # We normalize by total games to handle any format
        # A 6-0 6-0 has diff=12, total=12, ratio=1.0
        # A 7-6 7-6 has diff=2, total=26, ratio~=0.077
        dominance_ratio = Decimal(str(game_diff)) / Decimal(str(total_games))

    # Apply the multiplier formula
    # multiplier = base + dominance_ratio * scale
    multiplier = base + dominance_ratio * scale

    # Clamp to reasonable range [0.5, 2.0] to prevent extreme values
    multiplier = max(Decimal("0.5"), min(Decimal("2.0"), multiplier))

    # Round for cleanliness
    multiplier = multiplier.quantize(Decimal("0.0001"))
    dominance_ratio = dominance_ratio.quantize(Decimal("0.0001"))

    return MarginResult(
        multiplier=multiplier,
        games_won_winner=games_won_winner,
        games_won_loser=games_won_loser,
        sets_won_winner=sets_won_winner,
        sets_won_loser=sets_won_loser,
        dominance_ratio=dominance_ratio,
    )
