"""
ELO rating calculator for tennis matches.

Implements the standard ELO formula adapted for tennis with:
- Tournament level-specific K and S factors
- Optional surface-specific ratings
- Proper decimal handling for accuracy

The ELO formula:
  Expected score: E_A = 1 / (1 + 10^((R_B - R_A) / S))
  New rating: R'_A = R_A + K * (actual - expected)

Where:
  R_A, R_B = Current ratings of players A and B
  K = How much ratings change (volatility factor)
  S = Spread factor (how rating difference maps to win probability)
"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from teelo.elo.constants import ELO_CONSTANTS, DEFAULT_ELO, get_constants_for_level


@dataclass
class EloUpdate:
    """
    Result of an ELO calculation.

    Contains all the information needed to update the database
    and understand what happened in the calculation.
    """
    # Ratings before the match
    player_a_before: Decimal
    player_b_before: Decimal

    # Ratings after the match
    player_a_after: Decimal
    player_b_after: Decimal

    # Expected win probabilities (before the match)
    expected_a: Decimal
    expected_b: Decimal

    # Who won
    winner: str  # 'A' or 'B'

    # What constants were used
    k_factor: int
    s_factor: int
    tournament_level: str

    @property
    def player_a_change(self) -> Decimal:
        """Rating change for player A."""
        return self.player_a_after - self.player_a_before

    @property
    def player_b_change(self) -> Decimal:
        """Rating change for player B."""
        return self.player_b_after - self.player_b_before

    @property
    def was_upset(self) -> bool:
        """Whether the lower-rated player won."""
        if self.winner == 'A':
            return self.player_a_before < self.player_b_before
        else:
            return self.player_b_before < self.player_a_before

    def __repr__(self) -> str:
        return (
            f"<EloUpdate(A: {self.player_a_before:.0f} -> {self.player_a_after:.0f}, "
            f"B: {self.player_b_before:.0f} -> {self.player_b_after:.0f}, "
            f"winner={self.winner})>"
        )


class EloCalculator:
    """
    Tennis-specific ELO rating calculator.

    Calculates rating changes after matches using tournament-level
    specific K and S factors that were optimized for tennis prediction.

    Usage:
        calculator = EloCalculator()

        # Calculate after a match
        result = calculator.calculate(
            elo_a=Decimal("2100"),  # Djokovic's rating
            elo_b=Decimal("1900"),  # Opponent's rating
            winner="A",             # Djokovic won
            tournament_level="Grand Slam",
        )

        print(f"Djokovic: {result.player_a_before} -> {result.player_a_after}")
        print(f"Expected win prob: {result.expected_a:.1%}")
    """

    def __init__(self, constants: Optional[dict] = None):
        """
        Initialize the calculator.

        Args:
            constants: Optional custom ELO constants. If not provided,
                      uses the optimized constants from v3.0.
        """
        self.constants = constants or ELO_CONSTANTS

    def calculate(
        self,
        elo_a: Decimal,
        elo_b: Decimal,
        winner: str,
        tournament_level: str,
        surface: Optional[str] = None,  # Reserved for future surface-specific ELO
    ) -> EloUpdate:
        """
        Calculate new ELO ratings after a match.

        The calculation follows the standard ELO formula:
        1. Calculate expected score (win probability) for each player
        2. Compare actual result to expected
        3. Adjust ratings proportionally

        A player gains more rating points for an upset (beating someone
        higher-rated) and loses more for a surprising loss.

        Args:
            elo_a: Player A's rating before the match
            elo_b: Player B's rating before the match
            winner: 'A' if player A won, 'B' if player B won
            tournament_level: Tournament classification (e.g., 'Grand Slam')
            surface: Optional surface for surface-specific ratings (future use)

        Returns:
            EloUpdate with all calculation details

        Raises:
            ValueError: If winner is not 'A' or 'B'

        Example:
            # Djokovic (2100) beats qualifier (1600) at Australian Open
            result = calc.calculate(
                elo_a=Decimal("2100"),
                elo_b=Decimal("1600"),
                winner="A",
                tournament_level="Grand Slam",
            )
            # Djokovic gains ~14 points (he was heavily favored)
            # Qualifier loses ~14 points
        """
        if winner not in ("A", "B"):
            raise ValueError(f"winner must be 'A' or 'B', got '{winner}'")

        # Ensure we're working with Decimal
        elo_a = Decimal(str(elo_a))
        elo_b = Decimal(str(elo_b))

        # Get constants for this tournament level
        k, s = self._get_constants(tournament_level)
        k = Decimal(str(k))
        s = Decimal(str(s))

        # Calculate expected scores (win probabilities)
        # Formula: E_A = 1 / (1 + 10^((R_B - R_A) / S))
        # Using Decimal for precision
        rating_diff_a = (elo_b - elo_a) / s
        rating_diff_b = (elo_a - elo_b) / s

        # Calculate 10^(diff) carefully to avoid overflow
        # For very large differences, cap the expected score
        try:
            exp_a = Decimal("1") / (1 + Decimal("10") ** rating_diff_a)
            exp_b = Decimal("1") / (1 + Decimal("10") ** rating_diff_b)
        except:
            # Fallback for extreme rating differences
            if rating_diff_a > 0:
                exp_a = Decimal("0.001")  # Heavy underdog
                exp_b = Decimal("0.999")
            else:
                exp_a = Decimal("0.999")  # Heavy favorite
                exp_b = Decimal("0.001")

        # Actual scores (1 for win, 0 for loss)
        actual_a = Decimal("1") if winner == "A" else Decimal("0")
        actual_b = Decimal("1") - actual_a

        # Calculate new ratings
        # Formula: R'_A = R_A + K * (actual - expected)
        new_a = elo_a + k * (actual_a - exp_a)
        new_b = elo_b + k * (actual_b - exp_b)

        # Round to 2 decimal places for storage
        new_a = new_a.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        new_b = new_b.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Round expected scores to 4 decimal places (probability format)
        exp_a = exp_a.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        exp_b = exp_b.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

        return EloUpdate(
            player_a_before=elo_a,
            player_a_after=new_a,
            player_b_before=elo_b,
            player_b_after=new_b,
            expected_a=exp_a,
            expected_b=exp_b,
            winner=winner,
            k_factor=int(k),
            s_factor=int(s),
            tournament_level=tournament_level,
        )

    def get_win_probability(
        self,
        elo_a: Decimal,
        elo_b: Decimal,
        tournament_level: str = "ATP 250",
    ) -> Decimal:
        """
        Calculate the probability of player A winning.

        This is the expected score in ELO terminology.
        Useful for predictions and betting analysis.

        Args:
            elo_a: Player A's rating
            elo_b: Player B's rating
            tournament_level: Tournament level (affects S factor)

        Returns:
            Probability of player A winning (0 to 1)

        Example:
            prob = calc.get_win_probability(
                elo_a=Decimal("2100"),
                elo_b=Decimal("1800"),
                tournament_level="Grand Slam",
            )
            print(f"Win probability: {prob:.1%}")  # ~78%
        """
        elo_a = Decimal(str(elo_a))
        elo_b = Decimal(str(elo_b))

        _, s = self._get_constants(tournament_level)
        s = Decimal(str(s))

        rating_diff = (elo_b - elo_a) / s

        try:
            prob = Decimal("1") / (1 + Decimal("10") ** rating_diff)
        except:
            prob = Decimal("0.5")

        return prob.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    def rating_for_probability(
        self,
        target_prob: Decimal,
        opponent_elo: Decimal,
        tournament_level: str = "ATP 250",
    ) -> Decimal:
        """
        Calculate what rating you'd need for a given win probability.

        Useful for understanding "how good would someone need to be
        to have an X% chance of winning?"

        Args:
            target_prob: Desired win probability (0 to 1)
            opponent_elo: Opponent's rating
            tournament_level: Tournament level (affects S factor)

        Returns:
            Rating needed for that win probability

        Example:
            # What rating do you need for 90% chance vs 1800-rated player?
            needed = calc.rating_for_probability(
                target_prob=Decimal("0.90"),
                opponent_elo=Decimal("1800"),
                tournament_level="Grand Slam",
            )
            print(f"Need rating: {needed:.0f}")  # ~2150
        """
        import math

        target_prob = Decimal(str(target_prob))
        opponent_elo = Decimal(str(opponent_elo))

        _, s = self._get_constants(tournament_level)
        s = Decimal(str(s))

        # Rearranging ELO formula:
        # prob = 1 / (1 + 10^((R_B - R_A) / S))
        # 10^((R_B - R_A) / S) = (1 - prob) / prob
        # (R_B - R_A) / S = log10((1 - prob) / prob)
        # R_A = R_B - S * log10((1 - prob) / prob)

        if target_prob >= Decimal("1"):
            return opponent_elo + 1000  # Very high rating
        if target_prob <= Decimal("0"):
            return opponent_elo - 1000  # Very low rating

        odds_against = (Decimal("1") - target_prob) / target_prob
        log_odds = Decimal(str(math.log10(float(odds_against))))

        needed_elo = opponent_elo - s * log_odds

        return needed_elo.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _get_constants(self, level: str) -> tuple[int, int]:
        """
        Get K and S factors for a tournament level.

        Args:
            level: Tournament level name

        Returns:
            Tuple of (K, S) values
        """
        return get_constants_for_level(level)


def calculate_fast(
    elo_a: float,
    elo_b: float,
    winner: str,
    k_a: float,
    k_b: float,
    s: float,
) -> tuple[float, float, float]:
    """
    Float-only ELO calculation for optimization speed.

    Same math as EloCalculator.calculate() but uses plain floats instead of
    Decimal, and each player gets their own effective K (since boost/margin
    can differ per player).

    Args:
        elo_a: Player A's rating (float)
        elo_b: Player B's rating (float)
        winner: 'A' or 'B'
        k_a: Effective K-factor for player A (base_K * margin * boost_a)
        k_b: Effective K-factor for player B (base_K * margin * boost_b)
        s: Spread factor for this tournament level

    Returns:
        Tuple of (new_elo_a, new_elo_b, expected_winner_probability)
        The expected_winner_probability is the pre-match probability
        of the actual winner winning (used for log-loss computation).
    """
    # Expected score for player A: E_A = 1 / (1 + 10^((R_B - R_A) / S))
    try:
        exp_a = 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / s))
    except OverflowError:
        exp_a = 0.001 if elo_b > elo_a else 0.999

    exp_b = 1.0 - exp_a

    # Actual scores
    if winner == "A":
        actual_a, actual_b = 1.0, 0.0
        expected_winner_prob = exp_a
    else:
        actual_a, actual_b = 0.0, 1.0
        expected_winner_prob = exp_b

    # Update ratings (each player uses their own K)
    new_a = elo_a + k_a * (actual_a - exp_a)
    new_b = elo_b + k_b * (actual_b - exp_b)

    return new_a, new_b, expected_winner_prob


# Convenience function for simple usage
def calculate_elo_change(
    elo_a: float,
    elo_b: float,
    winner: str,
    tournament_level: str,
) -> tuple[float, float]:
    """
    Simple function to calculate new ELO ratings.

    For when you just need the new ratings without all the details.

    Args:
        elo_a: Player A's current rating
        elo_b: Player B's current rating
        winner: 'A' or 'B'
        tournament_level: Tournament level

    Returns:
        Tuple of (new_elo_a, new_elo_b)
    """
    calc = EloCalculator()
    result = calc.calculate(
        elo_a=Decimal(str(elo_a)),
        elo_b=Decimal(str(elo_b)),
        winner=winner,
        tournament_level=tournament_level,
    )
    return float(result.player_a_after), float(result.player_b_after)
