"""
Player identity management module.

This module handles the critical task of matching player names from
different sources (ATP, WTA, ITF, betting sites) to canonical player
records. This is essential for data quality.

Key components:
- PlayerIdentityService: Main service for finding/creating players
- PlayerMatcher: Fuzzy matching logic with confidence scores
- ReviewQueueManager: Handle unmatched players

The matching strategy (in priority order):
1. Exact external ID match (ATP/WTA/ITF ID)
2. Exact alias match (case-insensitive)
3. High-confidence fuzzy match (>0.98) - auto-match with new alias
4. Lower confidence - add to review queue with suggestions
"""

from teelo.players.identity import PlayerIdentityService, PlayerMatch
from teelo.players.aliases import normalize_name, compare_names

__all__ = [
    "PlayerIdentityService",
    "PlayerMatch",
    "normalize_name",
    "compare_names",
]
