"""Head-to-head feature group."""

from __future__ import annotations

from datetime import timedelta

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState


class H2HFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "h2h"

    def feature_names(self) -> list[str]:
        return [
            "h2h_a_wins",
            "h2h_b_wins",
            "h2h_a_wins_2y",
            "h2h_b_wins_2y",
            "h2h_a_wins_6m",
            "h2h_b_wins_6m",
            "h2h_a_wins_surface",
            "h2h_b_wins_surface",
            "h2h_a_wins_level",
            "h2h_b_wins_level",
            "h2h_total",
            "h2h_a_dominance",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        records_a = state_a.h2h[state_b.player_id]
        records_b = state_b.h2h[state_a.player_id]

        h2h_a_wins = float(sum(1 for record in records_a if record.won))
        h2h_b_wins = float(sum(1 for record in records_b if record.won))

        if ctx.match_date is None:
            h2h_a_wins_2y: float | None = None
            h2h_b_wins_2y: float | None = None
            h2h_a_wins_6m: float | None = None
            h2h_b_wins_6m: float | None = None
        else:
            cutoff_2y = ctx.match_date - timedelta(days=730)
            cutoff_6m = ctx.match_date - timedelta(days=182)

            h2h_a_wins_2y = float(
                sum(
                    1
                    for record in records_a
                    if record.won
                    and record.match_date is not None
                    and record.match_date >= cutoff_2y
                )
            )
            h2h_b_wins_2y = float(
                sum(
                    1
                    for record in records_b
                    if record.won
                    and record.match_date is not None
                    and record.match_date >= cutoff_2y
                )
            )
            h2h_a_wins_6m = float(
                sum(
                    1
                    for record in records_a
                    if record.won
                    and record.match_date is not None
                    and record.match_date >= cutoff_6m
                )
            )
            h2h_b_wins_6m = float(
                sum(
                    1
                    for record in records_b
                    if record.won
                    and record.match_date is not None
                    and record.match_date >= cutoff_6m
                )
            )

        if ctx.surface is None:
            h2h_a_wins_surface: float | None = None
            h2h_b_wins_surface: float | None = None
        else:
            h2h_a_wins_surface = float(
                sum(1 for record in records_a if record.won and record.surface == ctx.surface)
            )
            h2h_b_wins_surface = float(
                sum(1 for record in records_b if record.won and record.surface == ctx.surface)
            )

        h2h_a_wins_level = float(
            sum(1 for record in records_a if record.won and record.level_code == ctx.level_code)
        )
        h2h_b_wins_level = float(
            sum(1 for record in records_b if record.won and record.level_code == ctx.level_code)
        )

        h2h_total = float(h2h_a_wins + h2h_b_wins)
        h2h_a_dominance = (h2h_a_wins / h2h_total) if h2h_total > 0 else None

        return {
            "h2h_a_wins": h2h_a_wins,
            "h2h_b_wins": h2h_b_wins,
            "h2h_a_wins_2y": h2h_a_wins_2y,
            "h2h_b_wins_2y": h2h_b_wins_2y,
            "h2h_a_wins_6m": h2h_a_wins_6m,
            "h2h_b_wins_6m": h2h_b_wins_6m,
            "h2h_a_wins_surface": h2h_a_wins_surface,
            "h2h_b_wins_surface": h2h_b_wins_surface,
            "h2h_a_wins_level": h2h_a_wins_level,
            "h2h_b_wins_level": h2h_b_wins_level,
            "h2h_total": h2h_total,
            "h2h_a_dominance": h2h_a_dominance,
        }
