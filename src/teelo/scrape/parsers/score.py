"""
Tennis score parsing utilities.

Tennis scores are complex and come in many formats:
- Simple: "6-4 6-3"
- Tiebreak: "7-6(5) 6-4" or "7-6(7-5) 6-4"
- Super tiebreak: "6-4 4-6 10-8"
- Retirement: "6-4 2-1 RET" or "6-4 2-1 ret."
- Walkover: "W/O" or "walkover"
- Default: "DEF"

This module parses all these formats into a structured representation.
"""

import re
from dataclasses import dataclass
from typing import Optional


class ScoreParseError(Exception):
    """Raised when a score cannot be parsed."""
    pass


@dataclass
class SetScore:
    """
    Represents a single set score.

    Attributes:
        games_a: Games won by player A
        games_b: Games won by player B
        tiebreak_a: Player A's tiebreak score (if applicable)
        tiebreak_b: Player B's tiebreak score (if applicable)
        is_tiebreak: Whether this set went to a tiebreak
        is_super_tiebreak: Whether this is a super tiebreak (match tiebreak)
    """
    games_a: int
    games_b: int
    tiebreak_a: Optional[int] = None
    tiebreak_b: Optional[int] = None
    is_tiebreak: bool = False
    is_super_tiebreak: bool = False

    def __repr__(self) -> str:
        if self.is_tiebreak and self.tiebreak_b is not None:
            # Show the loser's tiebreak score (convention)
            tb_score = self.tiebreak_b if self.games_a > self.games_b else self.tiebreak_a
            return f"{self.games_a}-{self.games_b}({tb_score})"
        return f"{self.games_a}-{self.games_b}"


@dataclass
class ParsedScore:
    """
    Complete parsed score representation.

    Attributes:
        sets: List of SetScore objects
        winner: 'A' if player A won, 'B' if player B won, None if incomplete
        status: 'completed', 'retired', 'walkover', 'default', 'in_progress'
        retirement_set: Set number where retirement occurred (if applicable)
        raw_score: Original score string
    """
    sets: list[SetScore]
    winner: Optional[str] = None
    status: str = "completed"
    retirement_set: Optional[int] = None
    raw_score: str = ""

    def to_display_string(self) -> str:
        """Convert back to display format like '6-4 7-6(5)'."""
        parts = [str(s) for s in self.sets]
        if self.status == "retired":
            parts.append("RET")
        elif self.status == "walkover":
            return "W/O"
        elif self.status == "default":
            parts.append("DEF")
        return " ".join(parts)

    def to_structured(self) -> list[dict]:
        """
        Convert to structured format for database storage.

        Returns:
            List of dicts with 'a', 'b', and optionally 'tb_a', 'tb_b' keys
        """
        result = []
        for i, s in enumerate(self.sets):
            set_dict = {"a": s.games_a, "b": s.games_b}
            if s.is_tiebreak:
                set_dict["tb_a"] = s.tiebreak_a
                set_dict["tb_b"] = s.tiebreak_b
            if self.status == "retired" and self.retirement_set == i + 1:
                set_dict["retired"] = True
            result.append(set_dict)
        return result

    def __repr__(self) -> str:
        return f"<ParsedScore({self.to_display_string()}, winner={self.winner})>"


def parse_score(score_str: str) -> ParsedScore:
    """
    Parse a tennis score string into structured format.

    Handles all common score formats:
    - Regular sets: "6-4 6-3"
    - Tiebreaks: "7-6(5)" or "7-6(7-5)"
    - Super tiebreaks: "10-8" or "[10-8]"
    - Retirements: "6-4 2-1 RET"
    - Walkovers: "W/O"
    - Defaults: "DEF"

    Args:
        score_str: Raw score string from any source

    Returns:
        ParsedScore object with structured data

    Raises:
        ScoreParseError: If score cannot be parsed

    Examples:
        >>> parse_score("6-4 6-3")
        <ParsedScore(6-4 6-3, winner=A)>

        >>> parse_score("7-6(5) 3-6 7-6(7-3)")
        <ParsedScore(7-6(5) 3-6 7-6(3), winner=A)>

        >>> parse_score("6-4 2-1 RET")
        <ParsedScore(6-4 2-1 RET, winner=A)>
    """
    if not score_str:
        raise ScoreParseError("Empty score string")

    # Clean up the score string
    score = score_str.strip()
    original = score

    # Handle special cases first
    if _is_walkover(score):
        return ParsedScore(sets=[], winner="A", status="walkover", raw_score=original)

    if _is_default(score):
        return ParsedScore(sets=[], winner="A", status="default", raw_score=original)

    # Check for retirement
    is_retired, score = _extract_retirement(score)

    # Split into set components
    set_strings = _split_sets(score)

    if not set_strings:
        raise ScoreParseError(f"Could not parse score: {original}")

    # Parse each set
    sets = []
    for i, set_str in enumerate(set_strings):
        try:
            parsed_set = _parse_set(set_str)
            sets.append(parsed_set)
        except Exception as e:
            raise ScoreParseError(f"Could not parse set '{set_str}' in '{original}': {e}")

    # Determine winner
    winner = _determine_winner(sets, is_retired)

    # Create result
    result = ParsedScore(
        sets=sets,
        winner=winner,
        status="retired" if is_retired else "completed",
        retirement_set=len(sets) if is_retired else None,
        raw_score=original,
    )

    return result


def _is_walkover(score: str) -> bool:
    """Check if score indicates a walkover."""
    score_lower = score.lower().strip()
    return score_lower in ("w/o", "wo", "walkover", "w.o.", "w.o")


def _is_default(score: str) -> bool:
    """Check if score indicates a default."""
    score_lower = score.lower().strip()
    return score_lower in ("def", "default", "def.")


def _extract_retirement(score: str) -> tuple[bool, str]:
    """
    Check for and remove retirement indicator.

    Returns:
        (is_retired, cleaned_score)
    """
    # Common retirement patterns
    retirement_patterns = [
        r"\s*ret\.?\s*$",  # RET, ret, ret.
        r"\s*retired\.?\s*$",
        r"\s*r\.?\s*$",
        r"\s*\(ret\)\.?\s*$",
    ]

    for pattern in retirement_patterns:
        if re.search(pattern, score, re.IGNORECASE):
            cleaned = re.sub(pattern, "", score, flags=re.IGNORECASE)
            return True, cleaned.strip()

    return False, score


def _split_sets(score: str) -> list[str]:
    """
    Split score string into individual set strings.

    Handles space-separated and other formats.
    """
    # Remove any brackets around super tiebreaks first
    score = re.sub(r"\[(\d+-\d+)\]", r"\1", score)

    # Split by whitespace
    parts = score.split()

    # Filter out empty parts and non-score parts
    sets = []
    for part in parts:
        # Check if this looks like a set score
        if re.match(r"^\d+-\d+(\(\d+(-\d+)?\))?$", part):
            sets.append(part)

    return sets


def _parse_set(set_str: str) -> SetScore:
    """
    Parse a single set string.

    Handles:
    - "6-4" (regular set)
    - "7-6(5)" (tiebreak, loser's score shown)
    - "7-6(7-5)" (tiebreak, both scores shown)
    - "10-8" (super tiebreak)
    """
    # Match tiebreak format: 7-6(5) or 7-6(7-5)
    tb_match = re.match(r"^(\d+)-(\d+)\((\d+)(?:-(\d+))?\)$", set_str)

    if tb_match:
        games_a = int(tb_match.group(1))
        games_b = int(tb_match.group(2))
        tb_first = int(tb_match.group(3))
        tb_second = tb_match.group(4)

        if tb_second:
            # Both tiebreak scores given: 7-6(7-5)
            tb_a = tb_first
            tb_b = int(tb_second)
        else:
            # Only loser's score given: 7-6(5)
            # The winner has at least 7 (or 2 more than loser if loser >= 6)
            tb_loser = tb_first

            if games_a > games_b:
                # Player A won the tiebreak
                tb_b = tb_loser
                tb_a = max(7, tb_loser + 2)
            else:
                # Player B won the tiebreak
                tb_a = tb_loser
                tb_b = max(7, tb_loser + 2)

        return SetScore(
            games_a=games_a,
            games_b=games_b,
            tiebreak_a=tb_a,
            tiebreak_b=tb_b,
            is_tiebreak=True,
        )

    # Match regular set: 6-4
    regular_match = re.match(r"^(\d+)-(\d+)$", set_str)

    if regular_match:
        games_a = int(regular_match.group(1))
        games_b = int(regular_match.group(2))

        # Check if this is a super tiebreak (common in doubles or deciding sets)
        is_super_tb = games_a >= 10 or games_b >= 10

        return SetScore(
            games_a=games_a,
            games_b=games_b,
            is_super_tiebreak=is_super_tb,
        )

    raise ScoreParseError(f"Could not parse set: {set_str}")


def _determine_winner(sets: list[SetScore], is_retired: bool) -> Optional[str]:
    """
    Determine the match winner based on sets won.

    In tennis:
    - Best of 3: First to 2 sets
    - Best of 5: First to 3 sets

    If retired, the player who was winning when retirement happened wins.
    """
    if not sets:
        return None

    sets_a = 0
    sets_b = 0

    for s in sets:
        if s.games_a > s.games_b:
            sets_a += 1
        elif s.games_b > s.games_a:
            sets_b += 1
        # Tied sets shouldn't happen in completed matches

    # Check for clear winner
    if sets_a >= 2 and sets_a > sets_b:
        return "A"
    if sets_b >= 2 and sets_b > sets_a:
        return "B"

    # If retired, who was winning?
    if is_retired:
        if sets_a > sets_b:
            return "A"
        if sets_b > sets_a:
            return "B"

        # Sets tied - check last set games
        last_set = sets[-1]
        if last_set.games_a > last_set.games_b:
            return "A"
        if last_set.games_b > last_set.games_a:
            return "B"

        # Still tied - assume player A won (was listed first, usually winner)
        return "A"

    # Match possibly incomplete
    return None


def score_to_string(parsed: ParsedScore) -> str:
    """
    Convert ParsedScore back to display string.

    Alias for ParsedScore.to_display_string() for convenience.
    """
    return parsed.to_display_string()
