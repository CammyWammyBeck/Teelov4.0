"""Score profile, clutch classification, and clutch matchup features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_W8 = 8
_W64 = 64
_W128 = 128

# Clutch classification thresholds
_CLUTCH_HIGH = 0.55
_CLUTCH_LOW = 0.40


def _records(state: PlayerState, limit: int) -> list[MatchRecord]:
    return list(state.matches)[-limit:]


def _rate(numerator: float, denominator: int) -> float:
    if denominator == 0:
        return 0.5
    return numerator / denominator


def _average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _game_diff_avg(records: list[MatchRecord]) -> float:
    return _average([float(r.games_won - r.games_lost) for r in records])


def _set_diff_avg(records: list[MatchRecord]) -> float:
    return _average([float(r.sets_won - r.sets_lost) for r in records])


def _straight_sets_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.straight_sets), len(records))


def _deciding_set_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.deciding_set_played), len(records))


def _tiebreak_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.tiebreaks_played > 0), len(records))


def _tiebreak_win_rate(records: list[MatchRecord]) -> float:
    denom = sum(r.tiebreaks_played for r in records)
    if denom == 0:
        return 0.5
    return sum(r.tiebreaks_won for r in records) / denom


def _tiebreaks_played_count(records: list[MatchRecord]) -> float:
    return float(sum(r.tiebreaks_played for r in records))


def _close_match_rate(records: list[MatchRecord]) -> float:
    return _rate(sum(1.0 for r in records if r.close_match), len(records))


def _deciding_set_win_rate(records: list[MatchRecord]) -> float:
    ds = [r for r in records if r.deciding_set_played]
    if not ds:
        return 0.5
    return sum(1.0 for r in ds if r.won) / len(ds)


def _deciding_sets_played(records: list[MatchRecord]) -> float:
    return float(sum(1 for r in records if r.deciding_set_played))


def _comeback_rate(records: list[MatchRecord]) -> float:
    fsl = [r for r in records if r.first_set_lost]
    if not fsl:
        return 0.5
    return sum(1.0 for r in fsl if r.won) / len(fsl)


def _first_sets_lost(records: list[MatchRecord]) -> float:
    return float(sum(1 for r in records if r.first_set_lost))


def _straight_sets_win_rate(records: list[MatchRecord]) -> float:
    wins = [r for r in records if r.won]
    if not wins:
        return 0.5
    return sum(1.0 for r in wins if r.straight_sets) / len(wins)


def _clutch_bucket_stats(
    records: list[MatchRecord],
    low: float,
    high: float,
) -> dict[str, tuple[int, int, int]]:
    buckets: dict[str, tuple[int, int, int]] = {
        "clutch": (0, 0, 0),
        "normal": (0, 0, 0),
        "non_clutch": (0, 0, 0),
    }
    for r in records:
        if r.opponent_clutch_score is None:
            continue
        if r.opponent_clutch_score > high:
            key = "clutch"
        elif r.opponent_clutch_score < low:
            key = "non_clutch"
        else:
            key = "normal"
        w, l, t = buckets[key]
        if r.won:
            buckets[key] = (w + 1, l, t + 1)
        else:
            buckets[key] = (w, l + 1, t + 1)
    return buckets


class ScoreProfileFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "score_profile"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for suffix in ("a", "b"):
            for feat in (
                "game_diff_avg_8",
                "set_diff_avg_8",
                "straight_sets_rate_8",
                "deciding_set_rate_8",
                "tiebreak_rate_8",
                "tiebreak_win_rate_8",
                "tiebreaks_played_8",
                "close_match_rate_8",
            ):
                names.append(f"{feat}_{suffix}")
        for suffix in ("a", "b"):
            for feat in (
                "game_diff_avg_64",
                "set_diff_avg_64",
                "straight_sets_rate_64",
                "deciding_set_rate_64",
                "tiebreak_rate_64",
                "tiebreak_win_rate_64",
                "tiebreaks_played_64",
                "close_match_rate_64",
                "deciding_set_win_rate_64",
                "deciding_sets_played_64",
                "comeback_rate_64",
                "first_sets_lost_64",
                "straight_sets_win_rate_64",
            ):
                names.append(f"{feat}_{suffix}")
        for suffix in ("a", "b"):
            for feat in (
                "vs_clutch_win_rate",
                "vs_clutch_matches",
                "vs_normal_clutch_win_rate",
                "vs_normal_clutch_matches",
                "vs_non_clutch_win_rate",
                "vs_non_clutch_matches",
                "opponent_clutch_score",
            ):
                names.append(f"{feat}_{suffix}")
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        for suffix, state in (("a", state_a), ("b", state_b)):
            r8 = _records(state, _W8)
            r64 = _records(state, _W64)
            r128 = _records(state, _W128)

            # Window 8
            features[f"game_diff_avg_8_{suffix}"] = _game_diff_avg(r8)
            features[f"set_diff_avg_8_{suffix}"] = _set_diff_avg(r8)
            features[f"straight_sets_rate_8_{suffix}"] = _straight_sets_rate(r8)
            features[f"deciding_set_rate_8_{suffix}"] = _deciding_set_rate(r8)
            features[f"tiebreak_rate_8_{suffix}"] = _tiebreak_rate(r8)
            features[f"tiebreak_win_rate_8_{suffix}"] = _tiebreak_win_rate(r8)
            features[f"tiebreaks_played_8_{suffix}"] = _tiebreaks_played_count(r8)
            features[f"close_match_rate_8_{suffix}"] = _close_match_rate(r8)

            # Window 64
            features[f"game_diff_avg_64_{suffix}"] = _game_diff_avg(r64)
            features[f"set_diff_avg_64_{suffix}"] = _set_diff_avg(r64)
            features[f"straight_sets_rate_64_{suffix}"] = _straight_sets_rate(r64)
            features[f"deciding_set_rate_64_{suffix}"] = _deciding_set_rate(r64)
            features[f"tiebreak_rate_64_{suffix}"] = _tiebreak_rate(r64)
            features[f"tiebreak_win_rate_64_{suffix}"] = _tiebreak_win_rate(r64)
            features[f"tiebreaks_played_64_{suffix}"] = _tiebreaks_played_count(r64)
            features[f"close_match_rate_64_{suffix}"] = _close_match_rate(r64)
            features[f"deciding_set_win_rate_64_{suffix}"] = _deciding_set_win_rate(r64)
            features[f"deciding_sets_played_64_{suffix}"] = _deciding_sets_played(r64)
            features[f"comeback_rate_64_{suffix}"] = _comeback_rate(r64)
            features[f"first_sets_lost_64_{suffix}"] = _first_sets_lost(r64)
            features[f"straight_sets_win_rate_64_{suffix}"] = _straight_sets_win_rate(r64)

            # Clutch matchup
            buckets = _clutch_bucket_stats(r128, _CLUTCH_LOW, _CLUTCH_HIGH)
            for bucket_key, feat_prefix in (
                ("clutch", "vs_clutch"),
                ("normal", "vs_normal_clutch"),
                ("non_clutch", "vs_non_clutch"),
            ):
                w, _l, t = buckets[bucket_key]
                features[f"{feat_prefix}_matches_{suffix}"] = float(t)
                features[f"{feat_prefix}_win_rate_{suffix}"] = (
                    w / t if t > 0 else 0.5
                )

            # Opponent's current clutch score
            opp = state_b if suffix == "a" else state_a
            features[f"opponent_clutch_score_{suffix}"] = opp.clutch_score

        return features
