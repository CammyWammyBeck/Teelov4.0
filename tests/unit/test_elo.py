"""
Unit tests for ELO calculator.

Tests the core ELO calculation logic to ensure:
- Favorites winning gain less than underdogs winning
- Sum of rating changes is zero (zero-sum game)
- Tournament levels affect K and S factors correctly
- Edge cases are handled properly
"""

from decimal import Decimal

import pytest

from teelo.elo.calculator import EloCalculator, calculate_elo_change
from teelo.elo.constants import DEFAULT_ELO


class TestEloCalculator:
    """Tests for EloCalculator class."""

    @pytest.fixture
    def calculator(self):
        """Create a calculator instance for tests."""
        return EloCalculator()

    def test_favorite_wins(self, calculator):
        """
        Test that when the favorite wins, they gain fewer points.

        If player A has a higher rating, they're expected to win.
        Winning as the favorite should result in smaller point gain.
        """
        result = calculator.calculate(
            elo_a=Decimal("1800"),  # Favorite
            elo_b=Decimal("1600"),  # Underdog
            winner="A",
            tournament_level="ATP 250",
        )

        # Favorite won - should gain points
        assert result.player_a_after > result.player_a_before
        assert result.player_b_after < result.player_b_before

        # But not too many (favorite was expected to win)
        change = result.player_a_change
        assert change < 50  # Reasonable gain for favorite

    def test_underdog_wins(self, calculator):
        """
        Test that when the underdog wins, they gain more points.

        Beating a higher-rated player should result in bigger gains.
        """
        result = calculator.calculate(
            elo_a=Decimal("1600"),  # Underdog
            elo_b=Decimal("1800"),  # Favorite
            winner="A",  # Underdog wins!
            tournament_level="ATP 250",
        )

        # Underdog won - should gain significant points
        assert result.player_a_after > result.player_a_before
        assert result.player_b_after < result.player_b_before

        # Bigger gain than favorite winning
        change = result.player_a_change
        assert change > 50  # Good gain for upset

    def test_zero_sum(self, calculator):
        """
        Test that ELO changes are zero-sum.

        Whatever one player gains, the other should lose.
        This maintains the overall rating pool.
        """
        result = calculator.calculate(
            elo_a=Decimal("1700"),
            elo_b=Decimal("1650"),
            winner="A",
            tournament_level="Grand Slam",
        )

        # Total change should be zero (or very close due to rounding)
        total_change = result.player_a_change + result.player_b_change
        assert abs(total_change) < Decimal("0.1")

    def test_equal_ratings(self, calculator):
        """
        Test match between equally rated players.

        Each player should have ~50% expected win probability.
        Winner gains, loser loses by same amount.
        """
        result = calculator.calculate(
            elo_a=Decimal("1500"),
            elo_b=Decimal("1500"),
            winner="A",
            tournament_level="ATP 250",
        )

        # Expected probabilities should be ~50%
        assert Decimal("0.49") < result.expected_a < Decimal("0.51")
        assert Decimal("0.49") < result.expected_b < Decimal("0.51")

        # Changes should be equal and opposite
        assert abs(result.player_a_change + result.player_b_change) < Decimal("0.1")

    def test_grand_slam_vs_atp250(self, calculator):
        """
        Test that tournament level affects calculations.

        Different tournaments use different K and S factors.
        """
        same_ratings = (Decimal("1700"), Decimal("1600"))

        gs_result = calculator.calculate(
            elo_a=same_ratings[0],
            elo_b=same_ratings[1],
            winner="A",
            tournament_level="Grand Slam",
        )

        atp_result = calculator.calculate(
            elo_a=same_ratings[0],
            elo_b=same_ratings[1],
            winner="A",
            tournament_level="ATP 250",
        )

        # Results should be different due to different K/S factors
        assert gs_result.k_factor != atp_result.k_factor

    def test_win_probability(self, calculator):
        """
        Test win probability calculation.

        Higher-rated player should have higher win probability.
        """
        # Big rating difference
        prob = calculator.get_win_probability(
            elo_a=Decimal("2000"),
            elo_b=Decimal("1500"),
            tournament_level="ATP 250",
        )

        assert prob > Decimal("0.8")  # Should be heavily favored

        # Equal ratings
        prob_equal = calculator.get_win_probability(
            elo_a=Decimal("1500"),
            elo_b=Decimal("1500"),
            tournament_level="ATP 250",
        )

        assert Decimal("0.49") < prob_equal < Decimal("0.51")

    def test_invalid_winner(self, calculator):
        """Test that invalid winner raises error."""
        with pytest.raises(ValueError):
            calculator.calculate(
                elo_a=Decimal("1500"),
                elo_b=Decimal("1500"),
                winner="C",  # Invalid!
                tournament_level="ATP 250",
            )

    def test_was_upset_property(self, calculator):
        """Test the was_upset property."""
        # Favorite wins - not an upset
        result1 = calculator.calculate(
            elo_a=Decimal("1800"),
            elo_b=Decimal("1600"),
            winner="A",
            tournament_level="ATP 250",
        )
        assert not result1.was_upset

        # Underdog wins - upset!
        result2 = calculator.calculate(
            elo_a=Decimal("1600"),
            elo_b=Decimal("1800"),
            winner="A",
            tournament_level="ATP 250",
        )
        assert result2.was_upset


class TestConvenienceFunction:
    """Tests for the calculate_elo_change convenience function."""

    def test_calculate_elo_change(self):
        """Test the simple convenience function."""
        new_a, new_b = calculate_elo_change(
            elo_a=1700.0,
            elo_b=1600.0,
            winner="A",
            tournament_level="ATP 250",
        )

        # Winner gained, loser lost
        assert new_a > 1700.0
        assert new_b < 1600.0

        # Floats returned
        assert isinstance(new_a, float)
        assert isinstance(new_b, float)
