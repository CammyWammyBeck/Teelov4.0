"""Core state data structures for tennis match prediction."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date
from typing import NamedTuple


class MatchRecord(NamedTuple):
    temporal_order: int
    won: bool
    surface: str | None
    level_code: str
    games_won: int
    games_lost: int
    tournament_edition_id: int | None
    tournament_id: int | None
    match_date: date | None
    opponent_id: int
    opponent_elo: float | None = None
    opponent_surface_elo: float | None = None
    expected_win_prob: float | None = None
    sets_won: int = 0
    sets_lost: int = 0
    tiebreaks_played: int = 0
    tiebreaks_won: int = 0
    deciding_set_played: bool = False
    straight_sets: bool = False
    close_match: bool = False


class H2HRecord(NamedTuple):
    temporal_order: int
    won: bool
    surface: str | None
    level_code: str
    match_date: date | None


@dataclass
class MatchContext:
    match_id: int
    match_date: date | None
    surface: str | None
    level_code: str
    tour: str | None
    gender: str | None
    round: str | None
    year: int | None
    seed_a: int | None
    seed_b: int | None
    temporal_order: int | None
    tournament_edition_id: int | None
    tournament_id: int | None = None
    match_date_estimated: bool = False


@dataclass
class PlayerState:
    player_id: int
    elo_current: float = 1500.0
    elo_peak: float = 1500.0
    elo_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=200))
    surface_elo: dict[str, float] = field(default_factory=dict)
    surface_elo_peak: dict[str, float] = field(default_factory=dict)
    matches: deque[MatchRecord] = field(default_factory=lambda: deque(maxlen=1024))
    first_match_date: date | None = None
    last_match_date: date | None = None
    wins_total: int = 0
    losses_total: int = 0
    surface_wins: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    surface_losses: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    level_wins: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    level_losses: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    h2h: dict[int, list[H2HRecord]] = field(default_factory=lambda: defaultdict(list))
    tournament_matches: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    tournament_wins: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    tournament_losses: dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def update(self, record: MatchRecord, elo_post: float, surface_elo_post: float | None) -> None:
        self.matches.append(record)

        self.elo_current = elo_post
        self.elo_peak = max(self.elo_peak, elo_post)
        self.elo_history.append((record.temporal_order, elo_post))

        if surface_elo_post is not None and record.surface is not None:
            self.surface_elo[record.surface] = surface_elo_post
            current_peak = self.surface_elo_peak.get(record.surface, 1500.0)
            self.surface_elo_peak[record.surface] = max(current_peak, surface_elo_post)

        if record.won:
            self.wins_total += 1
            self.level_wins[record.level_code] += 1
            if record.surface is not None:
                self.surface_wins[record.surface] += 1
        else:
            self.losses_total += 1
            self.level_losses[record.level_code] += 1
            if record.surface is not None:
                self.surface_losses[record.surface] += 1

        if record.tournament_id is not None:
            self.tournament_matches[record.tournament_id] += 1
            if record.won:
                self.tournament_wins[record.tournament_id] += 1
            else:
                self.tournament_losses[record.tournament_id] += 1

        if record.match_date is not None:
            if self.first_match_date is None or record.match_date < self.first_match_date:
                self.first_match_date = record.match_date
            if self.last_match_date is None or record.match_date > self.last_match_date:
                self.last_match_date = record.match_date

        self.h2h[record.opponent_id].append(
            H2HRecord(
                temporal_order=record.temporal_order,
                won=record.won,
                surface=record.surface,
                level_code=record.level_code,
                match_date=record.match_date,
            )
        )

    def has_observed_surface_elo(self, surface: str | None) -> bool:
        if surface is None:
            return False
        return surface in self.surface_elo
