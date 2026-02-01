"""
ELO rating system constants.

These K and S factors were optimized via genetic algorithm in Teelo v3.0.
They can be re-optimized with more data if needed.

K factor: Controls rating volatility (how much ratings change per match)
  - Higher K = bigger rating swings
  - Lower K = more stable ratings

S factor: Controls the spread (how rating differences translate to win probability)
  - Higher S = smaller differences needed for high win probability
  - Lower S = larger differences needed

The values vary by tournament level because:
- Grand Slams (best of 5) are more likely to have favorites win
- Futures/ITF have higher variance due to less experienced players
- Different tournament levels attract different caliber of competition
"""

# Optimized constants from v3.0 genetic algorithm
# Format: {"K_{level}": k_value, "S_{level}": s_value}
ELO_CONSTANTS = {
    # Futures/ITF - Highest volatility
    # These tournaments have the most variability in player quality
    "K_F": 183,
    "S_F": 1241,

    # Challenger - High volatility
    # Bridge between ITF and main tour, still significant variance
    "K_C": 137,
    "S_C": 1441,

    # ATP 250/500 - Moderate volatility
    # Main tour events with mix of top players and qualifiers
    "K_A": 108,
    "S_A": 1670,

    # Masters 1000 - Lower volatility
    # Top players required to play, favorites usually win
    "K_M": 107,
    "S_M": 1809,

    # Grand Slam - Moderate volatility
    # Best of 5 format favors favorites, but upsets still happen
    "K_G": 116,
    "S_G": 1428,
}


# Default starting ELO for new players
DEFAULT_ELO = 1500

# Mapping from tournament level names to constant codes
LEVEL_TO_CODE = {
    # Grand Slams
    "Grand Slam": "G",

    # Masters
    "Masters 1000": "M",
    "WTA 1000": "M",

    # Mid-tier
    "ATP 500": "A",
    "WTA 500": "A",

    # Lower tier
    "ATP 250": "A",
    "WTA 250": "A",

    # Other main tour
    "ATP Finals": "M",  # Year-end championship treated as Masters level
    "WTA Finals": "M",

    # Challenger
    "Challenger": "C",
    "ATP Challenger": "C",
    "WTA 125": "C",

    # ITF/Futures
    "ITF": "F",
    "Future": "F",
    "ITF M25": "F",
    "ITF M15": "F",
    "ITF W25": "F",
    "ITF W15": "F",
}


# Default parameters for margin-of-victory K-factor scaling
# margin_base: baseline multiplier (1.0 = no effect for average match)
# margin_scale: how much dominance amplifies/reduces the multiplier
MARGIN_DEFAULTS = {
    "margin_base": 0.85,
    "margin_scale": 0.3,
}

# Default parameters for inactivity decay
# Pulls inactive players' ratings toward DEFAULT_ELO over time
# decay_rate: exponential decay rate (per year of excess inactivity)
# decay_start_days: days of inactivity before decay begins
DECAY_DEFAULTS = {
    "decay_rate": 0.05,
    "decay_start_days": 60,
}

# Default parameters for K-factor boost on new/returning players
# New players: higher K for faster convergence to true rating
# Returning players: higher K because form is uncertain after absence
BOOST_DEFAULTS = {
    "new_threshold": 30,       # Matches before player is "established"
    "new_boost": 1.5,          # K multiplier for brand-new players
    "returning_days": 180,     # Days absent before "returning" boost applies
    "returning_boost": 1.3,    # K multiplier for returning players
}


def get_constants_for_level(level: str) -> tuple[int, int]:
    """
    Get K and S constants for a tournament level.

    Args:
        level: Tournament level name (e.g., "Grand Slam", "ATP 250")

    Returns:
        Tuple of (K, S) values

    Example:
        k, s = get_constants_for_level("Grand Slam")
        # k = 116, s = 1428
    """
    code = LEVEL_TO_CODE.get(level, "A")  # Default to ATP 250 level
    k = ELO_CONSTANTS[f"K_{code}"]
    s = ELO_CONSTANTS[f"S_{code}"]
    return k, s
