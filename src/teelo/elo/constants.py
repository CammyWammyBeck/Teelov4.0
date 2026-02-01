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
# Men's constants (prefix: K_, S_)
# Women's constants (prefix: K_W, S_W) — initially same as men's,
# to be separately optimized since women's matches are always best-of-3
ELO_CONSTANTS = {
    # --- Men's constants ---

    # Futures/ITF - Highest volatility
    "K_F": 183,
    "S_F": 1241,

    # Challenger - High volatility
    "K_C": 137,
    "S_C": 1441,

    # ATP 250/500 - Moderate volatility
    "K_A": 108,
    "S_A": 1670,

    # Masters 1000 - Lower volatility
    "K_M": 107,
    "S_M": 1809,

    # Grand Slam - Best of 5 format favors favorites
    "K_G": 116,
    "S_G": 1428,

    # --- Women's constants ---
    # Separate from men's because all women's matches are best-of-3
    # (including Grand Slams), leading to higher variance

    # ITF Women
    "K_WF": 183,
    "S_WF": 1241,

    # WTA 125 (Challenger equivalent)
    "K_WC": 137,
    "S_WC": 1441,

    # WTA 250/500
    "K_WA": 108,
    "S_WA": 1670,

    # WTA 1000
    "K_WM": 107,
    "S_WM": 1809,

    # Grand Slam (Women's) - Best of 3, more upsets than men's GS
    "K_WG": 116,
    "S_WG": 1428,
}


# Default starting ELO for new players
DEFAULT_ELO = 1500

# Mapping from (tournament level, tour) to constant codes.
# Women's levels use "W" prefix (WF, WC, WA, WM, WG) so they get
# separate K/S constants from men's levels (F, C, A, M, G).
#
# The tour-aware function get_level_code() should be preferred over
# this dict for new code. This dict is kept for backward compatibility
# and maps level names to men's codes by default.
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

# Mapping from level name to base code (without gender prefix)
# Used by get_level_code() to then apply the women's "W" prefix
_LEVEL_TO_BASE_CODE = {
    "Grand Slam": "G",
    "Masters 1000": "M",
    "WTA 1000": "M",
    "ATP 500": "A",
    "WTA 500": "A",
    "ATP 250": "A",
    "WTA 250": "A",
    "ATP Finals": "M",
    "WTA Finals": "M",
    "Challenger": "C",
    "ATP Challenger": "C",
    "WTA 125": "C",
    "ITF": "F",
    "Future": "F",
    "ITF M25": "F",
    "ITF M15": "F",
    "ITF W25": "F",
    "ITF W15": "F",
}

# Tours considered women's — these get the "W" prefix for constants
WOMENS_TOURS = {"WTA"}


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


def get_level_code(level: str, tour: str | None = None) -> str:
    """
    Get the ELO constant code for a tournament level, accounting for gender.

    Women's tours get a "W" prefix (e.g., "WG" instead of "G") so they
    use separate K/S constants from men's tours.

    Args:
        level: Tournament level name (e.g., "Grand Slam", "WTA 250")
        tour: Tour name (e.g., "ATP", "WTA"). If None, infers from level name.

    Returns:
        Level code string (e.g., "G", "WG", "A", "WA")
    """
    base_code = _LEVEL_TO_BASE_CODE.get(level, "A")

    # Determine if this is a women's tour
    is_womens = (
        (tour is not None and tour.upper() in WOMENS_TOURS)
        or level.startswith("WTA")
        or level.startswith("ITF W")
    )

    if is_womens:
        return f"W{base_code}"
    return base_code


def get_constants_for_level(level: str, tour: str | None = None) -> tuple[int, int]:
    """
    Get K and S constants for a tournament level.

    Args:
        level: Tournament level name (e.g., "Grand Slam", "ATP 250")
        tour: Tour name (e.g., "ATP", "WTA"). Used to select men's or women's constants.

    Returns:
        Tuple of (K, S) values

    Example:
        k, s = get_constants_for_level("Grand Slam")          # Men's: k=116, s=1428
        k, s = get_constants_for_level("Grand Slam", "WTA")   # Women's: k=116, s=1428
    """
    code = get_level_code(level, tour)
    k = ELO_CONSTANTS[f"K_{code}"]
    s = ELO_CONSTANTS[f"S_{code}"]
    return k, s
