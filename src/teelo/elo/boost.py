"""
K-factor boost for new and returning players.

New players need their ratings to converge quickly — a player who's 2-0 shouldn't
be stuck near 1500 for dozens of matches. We boost K for players with few matches.

Returning players have uncertain form after a long absence, so we also boost K
to let their rating adjust faster when they come back.

The boost is a multiplier on the base K-factor:
- New player boost: linearly interpolates from new_boost → 1.0 as match count grows
- Returning player boost: flat multiplier if absence exceeds threshold
- The two boosts are combined multiplicatively
- Final result is clamped to [1.0, 3.0] for safety
"""

from teelo.elo.constants import BOOST_DEFAULTS


def calculate_k_boost(
    total_matches: int,
    days_since_last_match: float | None,
    new_threshold: int | None = None,
    new_boost: float | None = None,
    returning_days: float | None = None,
    returning_boost: float | None = None,
) -> float:
    """
    Calculate a K-factor multiplier for new or returning players.

    Args:
        total_matches: How many completed matches this player has in the system
        days_since_last_match: Days since their last match, or None if first match
        new_threshold: Match count at which new-player boost fully fades.
                      Default from BOOST_DEFAULTS.
        new_boost: K multiplier for brand-new players (0 matches).
                  Linearly fades to 1.0 at new_threshold matches.
                  Default from BOOST_DEFAULTS.
        returning_days: Days of absence that triggers the returning boost.
                       Default from BOOST_DEFAULTS.
        returning_boost: K multiplier for returning players.
                        Default from BOOST_DEFAULTS.

    Returns:
        K-factor multiplier, clamped to [1.0, 3.0]

    Examples:
        # Brand new player — full boost
        calculate_k_boost(0, None)  # → 1.5

        # Established player, active — no boost
        calculate_k_boost(100, 7.0)  # → 1.0

        # Established player returning after long absence
        calculate_k_boost(100, 200.0)  # → 1.3
    """
    if new_threshold is None:
        new_threshold = BOOST_DEFAULTS["new_threshold"]
    if new_boost is None:
        new_boost = BOOST_DEFAULTS["new_boost"]
    if returning_days is None:
        returning_days = BOOST_DEFAULTS["returning_days"]
    if returning_boost is None:
        returning_boost = BOOST_DEFAULTS["returning_boost"]

    multiplier = 1.0

    # New player boost: linearly interpolate from new_boost → 1.0
    if total_matches < new_threshold and new_threshold > 0:
        # At 0 matches: full new_boost. At new_threshold matches: 1.0
        progress = total_matches / new_threshold
        multiplier *= new_boost + (1.0 - new_boost) * progress

    # Returning player boost: flat multiplier if absent long enough
    if days_since_last_match is not None and days_since_last_match > returning_days:
        multiplier *= returning_boost

    # Clamp to safe range
    return max(1.0, min(3.0, multiplier))
