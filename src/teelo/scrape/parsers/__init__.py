"""
Parsers for scraped tennis data.

This module contains parsers for:
- Score parsing (handling tiebreaks, retirements, walkovers)
- Player name extraction from various HTML structures
"""

from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.scrape.parsers.player import extract_player_info

__all__ = [
    "parse_score",
    "ScoreParseError",
    "extract_player_info",
]
