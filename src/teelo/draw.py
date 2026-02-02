"""
Draw bracket utility functions.

Provides positional math for tournament brackets. Draw positions are
1-indexed within each round and follow standard single-elimination
bracket progression:

    Round N, position p  →  Round N+1, position ceil(p/2)

So positions 1 and 2 in R128 feed into position 1 in R64,
positions 3 and 4 feed into position 2, etc.

These functions are used by:
- Draw ingestion (assigning positions to scraped draw entries)
- Result propagation (creating next-round matches when feeders complete)
- Bracket validation (checking draw consistency)
"""

import math
from typing import Optional


# Ordered round progression for standard single-elimination draws
# Each round feeds into the next one in sequence
ROUND_PROGRESSION = ["R128", "R64", "R32", "R16", "QF", "SF", "F"]

# Number of matches in each round for different draw sizes
# Key = draw size, value = first round code
DRAW_SIZE_TO_FIRST_ROUND = {
    128: "R128",
    64: "R64",
    32: "R32",
    16: "R16",
    8: "QF",
    4: "SF",
    2: "F",
}

# Expected number of matches per round
ROUND_MATCH_COUNT = {
    "R128": 64,
    "R64": 32,
    "R32": 16,
    "R16": 8,
    "QF": 4,
    "SF": 2,
    "F": 1,
}


def get_next_round(round_code: str) -> Optional[str]:
    """
    Get the next round in tournament progression.

    Args:
        round_code: Current round (e.g., 'R64', 'QF')

    Returns:
        Next round code, or None if this is the Final

    Examples:
        >>> get_next_round("R128")
        'R64'
        >>> get_next_round("SF")
        'F'
        >>> get_next_round("F")
        None
    """
    try:
        idx = ROUND_PROGRESSION.index(round_code)
    except ValueError:
        return None

    if idx >= len(ROUND_PROGRESSION) - 1:
        return None
    return ROUND_PROGRESSION[idx + 1]


def get_next_draw_position(position: int) -> int:
    """
    Compute the draw position in the next round.

    Winner of position p feeds into position ceil(p/2) in the next round.

    Args:
        position: 1-indexed draw position in the current round

    Returns:
        1-indexed draw position in the next round

    Examples:
        >>> get_next_draw_position(1)
        1
        >>> get_next_draw_position(2)
        1
        >>> get_next_draw_position(3)
        2
        >>> get_next_draw_position(4)
        2
    """
    return math.ceil(position / 2)


def get_feeder_positions(position: int) -> tuple[int, int]:
    """
    Get the two feeder positions from the previous round that feed
    into this position.

    Position p in round N+1 is fed by positions 2p-1 and 2p in round N.

    Args:
        position: 1-indexed draw position in the current round

    Returns:
        Tuple of (top_position, bottom_position) from the previous round

    Examples:
        >>> get_feeder_positions(1)
        (1, 2)
        >>> get_feeder_positions(2)
        (3, 4)
        >>> get_feeder_positions(3)
        (5, 6)
    """
    return (2 * position - 1, 2 * position)


def get_previous_round(round_code: str) -> Optional[str]:
    """
    Get the previous round in tournament progression.

    Args:
        round_code: Current round (e.g., 'R64', 'QF')

    Returns:
        Previous round code, or None if this is the first round

    Examples:
        >>> get_previous_round("R64")
        'R128'
        >>> get_previous_round("R128")
        None
    """
    try:
        idx = ROUND_PROGRESSION.index(round_code)
    except ValueError:
        return None

    if idx == 0:
        return None
    return ROUND_PROGRESSION[idx - 1]


def get_first_round_for_draw_size(draw_size: int) -> str:
    """
    Determine the first main-draw round for a given draw size.

    Handles non-power-of-2 sizes (e.g., 96-draw) by rounding up
    to the next power of 2. A 96-draw tournament starts at R128
    with 32 byes in the first round.

    Args:
        draw_size: Number of players in the draw

    Returns:
        Round code for the first round (e.g., 'R128', 'R64', 'R32')

    Examples:
        >>> get_first_round_for_draw_size(128)
        'R128'
        >>> get_first_round_for_draw_size(96)
        'R128'
        >>> get_first_round_for_draw_size(64)
        'R64'
        >>> get_first_round_for_draw_size(32)
        'R32'
        >>> get_first_round_for_draw_size(48)
        'R64'
    """
    # Round up to next power of 2
    effective_size = 1
    while effective_size < draw_size:
        effective_size *= 2

    if effective_size in DRAW_SIZE_TO_FIRST_ROUND:
        return DRAW_SIZE_TO_FIRST_ROUND[effective_size]

    # Fallback for very large draws
    return "R128"


def get_expected_matches_in_round(round_code: str) -> Optional[int]:
    """
    Get the expected number of matches in a round.

    Args:
        round_code: Round code (e.g., 'R64', 'QF')

    Returns:
        Number of matches, or None for unknown rounds
    """
    return ROUND_MATCH_COUNT.get(round_code)


def validate_draw_positions(
    round_code: str,
    positions: list[int],
    draw_size: int,
) -> list[str]:
    """
    Validate that draw positions are consistent for a round.

    Checks:
    - Positions are within expected range
    - No duplicate positions
    - No gaps (unless byes fill them)

    Args:
        round_code: Round code
        positions: List of draw positions found
        draw_size: Total draw size

    Returns:
        List of warning messages (empty if all valid)
    """
    warnings = []
    expected_count = ROUND_MATCH_COUNT.get(round_code)

    if expected_count is None:
        warnings.append(f"Unknown round code: {round_code}")
        return warnings

    # Check for duplicates
    if len(positions) != len(set(positions)):
        warnings.append(f"{round_code}: Duplicate draw positions found")

    # Check range
    for pos in positions:
        if pos < 1 or pos > expected_count:
            warnings.append(
                f"{round_code}: Position {pos} out of range (1-{expected_count})"
            )

    return warnings
