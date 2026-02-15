"""
Inactivity decay for ELO ratings.

When a player hasn't played for a while, their rating becomes less reliable.
This module pulls inactive players' ratings toward the default (1500) using
exponential decay, reflecting increased uncertainty about their true level.

The decay only kicks in after a configurable number of inactive days,
so short breaks between tournaments have no effect.

Formula:
    new_rating = DEFAULT + (current - DEFAULT) * exp(-decay_rate * excess_days / 365)

Where excess_days = days_since_last_match - decay_start_days
"""

import math

from teelo.elo.constants import DEFAULT_ELO, DECAY_DEFAULTS


def apply_inactivity_decay(
    current_rating: float,
    days_since_last_match: float,
    decay_rate: float | None = None,
    decay_start_days: float | None = None,
    target_rating: float | None = None,
) -> float:
    """
    Apply inactivity decay to a player's rating, pulling toward DEFAULT_ELO.

    Only applies if the player has been inactive longer than decay_start_days.
    The decay is exponential — longer absences cause more regression, but
    the effect diminishes over time (never fully reaches DEFAULT_ELO).

    Args:
        current_rating: Player's current ELO rating
        days_since_last_match: Days since their last completed match
        decay_rate: Exponential decay rate per year of excess inactivity.
                    Higher = faster regression. Default from DECAY_DEFAULTS.
        decay_start_days: Days of inactivity before decay begins.
                         Default from DECAY_DEFAULTS.

    Returns:
        Decayed rating (unchanged if within the grace period)

    Examples:
        # Player inactive for 30 days (within grace period) — no change
        apply_inactivity_decay(1800.0, 30.0)  # → 1800.0

        # Player inactive for 200 days — some decay toward 1500
        apply_inactivity_decay(1800.0, 200.0)  # → ~1786
    """
    if decay_rate is None:
        decay_rate = DECAY_DEFAULTS["decay_rate"]
    if decay_start_days is None:
        decay_start_days = DECAY_DEFAULTS["decay_start_days"]
    if target_rating is None:
        target_rating = float(DEFAULT_ELO)

    # No decay if within grace period
    if days_since_last_match <= decay_start_days:
        return current_rating

    # Excess days beyond the grace period
    excess_days = days_since_last_match - decay_start_days

    # Exponential decay toward DEFAULT_ELO
    # Factor ranges from 1.0 (no decay) toward 0.0 (fully regressed)
    decay_factor = math.exp(-decay_rate * excess_days / 365.0)

    new_rating = target_rating + (current_rating - target_rating) * decay_factor

    return new_rating
