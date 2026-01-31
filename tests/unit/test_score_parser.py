"""
Unit tests for score parser.

Tests all the various tennis score formats:
- Regular sets: "6-4 6-3"
- Tiebreaks: "7-6(5)" or "7-6(7-5)"
- Super tiebreaks: "10-8"
- Retirements: "6-4 2-1 RET"
- Walkovers: "W/O"
"""

import pytest

from teelo.scrape.parsers.score import parse_score, ScoreParseError


class TestParseScore:
    """Tests for parse_score function."""

    def test_simple_straight_sets(self):
        """Test parsing a simple straight sets match."""
        result = parse_score("6-4 6-3")

        assert len(result.sets) == 2
        assert result.sets[0].games_a == 6
        assert result.sets[0].games_b == 4
        assert result.sets[1].games_a == 6
        assert result.sets[1].games_b == 3
        assert result.winner == "A"
        assert result.status == "completed"

    def test_three_sets(self):
        """Test parsing a three-set match."""
        result = parse_score("6-4 4-6 7-5")

        assert len(result.sets) == 3
        assert result.winner == "A"  # Won 2 sets to 1
        assert result.sets[0].games_a == 6
        assert result.sets[1].games_a == 4
        assert result.sets[2].games_a == 7

    def test_tiebreak_loser_score_only(self):
        """Test tiebreak with only loser's score shown (common format)."""
        result = parse_score("7-6(5) 6-4")

        assert len(result.sets) == 2
        assert result.sets[0].is_tiebreak
        assert result.sets[0].games_a == 7
        assert result.sets[0].games_b == 6
        # The (5) means loser scored 5 in tiebreak
        assert result.sets[0].tiebreak_b == 5
        assert result.sets[0].tiebreak_a >= 7  # Winner had at least 7

    def test_tiebreak_both_scores(self):
        """Test tiebreak with both scores shown."""
        result = parse_score("7-6(7-5) 6-4")

        assert result.sets[0].is_tiebreak
        assert result.sets[0].tiebreak_a == 7
        assert result.sets[0].tiebreak_b == 5

    def test_extended_tiebreak(self):
        """Test extended tiebreak (e.g., 9-7)."""
        result = parse_score("7-6(9-7) 6-4")

        assert result.sets[0].is_tiebreak
        assert result.sets[0].tiebreak_a == 9
        assert result.sets[0].tiebreak_b == 7

    def test_super_tiebreak(self):
        """Test match tiebreak (third set super tiebreak)."""
        result = parse_score("6-4 4-6 10-8")

        assert len(result.sets) == 3
        assert result.sets[2].games_a == 10
        assert result.sets[2].games_b == 8
        assert result.sets[2].is_super_tiebreak

    def test_retirement(self):
        """Test retirement mid-match."""
        result = parse_score("6-4 2-1 RET")

        assert result.status == "retired"
        assert result.retirement_set == 2  # Retired in 2nd set
        assert len(result.sets) == 2
        assert result.winner == "A"  # Player winning when opponent retired

    def test_retirement_various_formats(self):
        """Test various retirement format variations."""
        formats = ["6-4 2-1 ret", "6-4 2-1 ret.", "6-4 2-1 Ret", "6-4 2-1 RET."]

        for fmt in formats:
            result = parse_score(fmt)
            assert result.status == "retired", f"Failed for format: {fmt}"

    def test_walkover(self):
        """Test walkover (match not played)."""
        result = parse_score("W/O")

        assert result.status == "walkover"
        assert len(result.sets) == 0
        assert result.winner == "A"

    def test_walkover_variations(self):
        """Test various walkover formats."""
        for fmt in ["W/O", "wo", "WO", "walkover", "w.o."]:
            result = parse_score(fmt)
            assert result.status == "walkover", f"Failed for: {fmt}"

    def test_default(self):
        """Test default (disqualification)."""
        result = parse_score("DEF")

        assert result.status == "default"

    def test_five_set_match(self):
        """Test a five-set Grand Slam match."""
        result = parse_score("7-6(4) 6-7(5) 6-4 3-6 7-5")

        assert len(result.sets) == 5
        assert result.winner == "A"  # Won 3-2 in sets
        assert result.sets[0].is_tiebreak
        assert result.sets[1].is_tiebreak

    def test_winner_b(self):
        """Test when player B wins."""
        result = parse_score("4-6 3-6")

        assert result.winner == "B"
        assert result.sets[0].games_a == 4
        assert result.sets[0].games_b == 6

    def test_to_display_string(self):
        """Test converting parsed score back to string."""
        original = "6-4 7-6(5)"
        result = parse_score(original)

        display = result.to_display_string()
        assert "6-4" in display
        assert "7-6" in display

    def test_to_structured(self):
        """Test converting to structured format for database."""
        result = parse_score("6-4 7-6(7-5)")

        structured = result.to_structured()

        assert len(structured) == 2
        assert structured[0] == {"a": 6, "b": 4}
        assert structured[1]["a"] == 7
        assert structured[1]["b"] == 6
        assert structured[1]["tb_a"] == 7
        assert structured[1]["tb_b"] == 5

    def test_empty_score_raises(self):
        """Test that empty score raises error."""
        with pytest.raises(ScoreParseError):
            parse_score("")

    def test_invalid_score_raises(self):
        """Test that invalid score raises error."""
        with pytest.raises(ScoreParseError):
            parse_score("not a score")


class TestEdgeCases:
    """Edge case tests for score parser."""

    def test_first_set_retirement(self):
        """Test retirement in first set."""
        result = parse_score("3-2 RET")

        assert result.status == "retired"
        assert result.retirement_set == 1

    def test_love_set(self):
        """Test 6-0 (love set)."""
        result = parse_score("6-0 6-0")

        assert result.sets[0].games_a == 6
        assert result.sets[0].games_b == 0

    def test_double_bagel(self):
        """Test double bagel (two 6-0 sets)."""
        result = parse_score("6-0 6-0")

        assert len(result.sets) == 2
        assert all(s.games_b == 0 for s in result.sets)
        assert result.winner == "A"
