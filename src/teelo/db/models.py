"""
SQLAlchemy ORM models for Teelo.

This module defines all database tables and their relationships.
The schema is designed around a canonical player identity system
where each player has one record regardless of which tour they play on.

Key design decisions:
- Players have optional ATP/WTA/ITF IDs (cross-tour matching)
- Player aliases handle name variations from different sources
- Matches link to players via foreign keys (never raw names)
- Single unified matches table handles full lifecycle (scheduled -> completed)
- Features are stored separately for ML experimentation
- Scrape queue enables reliable retry logic

Tables:
- players: Canonical player records
- player_aliases: Name variations for matching
- player_review_queue: Unmatched players awaiting review
- tournaments: Tournament master data
- tournament_editions: Yearly tournament instances
- matches: All matches (scheduled, in progress, and completed)
- feature_sets: ML feature definitions (versioned)
- match_features: Computed features per match
- scrape_queue: Pending/failed scrape tasks
- update_log: System audit trail
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# =============================================================================
# Constants
# =============================================================================

# Round ordering for temporal_order computation
# Higher numbers = later in tournament progression
# Gaps allow for future additions without recomputing existing data
ROUND_ORDER: dict[str, int] = {
    # Qualifying rounds
    "Q1": 1,
    "Q2": 2,
    "Q3": 3,
    # Main draw rounds (standard ATP/WTA draw sizes)
    "R128": 10,
    "R64": 20,
    "R32": 30,
    "R16": 40,
    "QF": 50,
    "SF": 60,
    "F": 70,
    # Round robin (ATP Finals, etc.) - typically before knockouts
    "RR": 35,
}

# Round progress as fraction of tournament (0.0 = start, 1.0 = end)
# Used for estimating match dates from tournament start/end dates
ROUND_PROGRESS: dict[str, float] = {
    "Q1": 0.0,
    "Q2": 0.05,
    "Q3": 0.10,
    "R128": 0.15,
    "R64": 0.25,
    "R32": 0.40,
    "R16": 0.55,
    "QF": 0.70,
    "SF": 0.85,
    "F": 1.0,
    "RR": 0.50,  # Round robin typically mid-tournament
}


def estimate_match_date_from_round(
    round_code: str,
    tournament_start: datetime,
    tournament_end: datetime,
) -> Optional[datetime]:
    """
    Estimate a match date based on the round and tournament dates.

    Uses ROUND_PROGRESS to interpolate between tournament start and end dates.
    For example, a Final (progress=1.0) would be estimated at tournament_end,
    while R128 (progress=0.15) would be near the start.

    Args:
        round_code: Round code (F, SF, QF, R16, etc.)
        tournament_start: Tournament start date
        tournament_end: Tournament end date

    Returns:
        Estimated date, or None if dates are not available
    """
    if not tournament_start or not tournament_end:
        return None

    progress = ROUND_PROGRESS.get(round_code, 0.5)  # Default to mid-tournament

    # Handle date vs datetime
    if hasattr(tournament_start, 'date'):
        start = tournament_start if isinstance(tournament_start, datetime) else datetime.combine(tournament_start, datetime.min.time())
        end = tournament_end if isinstance(tournament_end, datetime) else datetime.combine(tournament_end, datetime.min.time())
    else:
        start = tournament_start
        end = tournament_end

    # Calculate estimated date
    duration = (end - start).days
    estimated_days = int(duration * progress)

    from datetime import timedelta
    estimated_date = start + timedelta(days=estimated_days)

    return estimated_date.date() if hasattr(estimated_date, 'date') else estimated_date


def compute_temporal_order(
    match_date: datetime,
    tournament_edition_id: int,
    round_code: str,
) -> int:
    """
    Compute a sortable integer for chronological match ordering.

    The temporal_order allows simple comparisons: if match_a.temporal_order < match_b.temporal_order,
    then match_a happened before match_b. This is critical for:
    - ML training (only use prior matches as features)
    - ELO calculations (update ratings in order)
    - Feature engineering (head-to-head, recent form)

    Format: YYYYMMDD_EEEEE_RR (as integer)
    - YYYYMMDD: Date (8 digits)
    - EEEEE: Tournament edition ID (5 digits, supports up to 99999 editions)
    - RR: Round order (2 digits)

    Args:
        match_date: Date the match was played
        tournament_edition_id: ID of the tournament edition
        round_code: Round code (F, SF, QF, R16, etc.)

    Returns:
        Integer suitable for chronological sorting
    """
    if match_date is None:
        # For scheduled matches without a date, use a far-future date
        year, month, day = 9999, 12, 31
    else:
        year = match_date.year
        month = match_date.month
        day = match_date.day

    # Get round order, default to 0 for unknown rounds
    round_num = ROUND_ORDER.get(round_code, 0)

    # Combine into single integer
    # Date: YYYYMMDD (8 digits) * 10^7 = positions 8-15
    # Edition: EEEEE (5 digits) * 10^2 = positions 3-7
    # Round: RR (2 digits) = positions 1-2
    temporal_order = (
        year * 10000_00000_00 +      # Year in positions 12-15
        month * 100_00000_00 +       # Month in positions 10-11
        day * 1_00000_00 +           # Day in positions 8-9
        (tournament_edition_id % 100000) * 100 +  # Edition in positions 3-7
        round_num                     # Round in positions 1-2
    )

    return temporal_order


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


# =============================================================================
# Player Models
# =============================================================================

class Player(Base):
    """
    Canonical player record.

    Each player has exactly one record in this table, regardless of which
    tours they play on (ATP, WTA, ITF). External IDs from each tour are
    stored as separate columns, allowing cross-tour player matching.

    The canonical_name is the "official" name we use for display.
    Actual name matching uses the player_aliases table.
    """
    __tablename__ = "players"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The "official" name for this player (used for display)
    canonical_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # External IDs from different tours (all optional - not all players have all IDs)
    # These are used for reliable matching when scraping from official sources
    atp_id: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True)
    wta_id: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True)
    itf_id: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True)

    # Demographics (populated from player profiles when available)
    nationality_ioc: Mapped[Optional[str]] = mapped_column(String(3), nullable=True)
    birth_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)
    turned_pro_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hand: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)  # 'Right', 'Left'
    backhand: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 'One-Handed', 'Two-Handed'
    height_cm: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    aliases: Mapped[list["PlayerAlias"]] = relationship(
        back_populates="player", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Player(id={self.id}, name='{self.canonical_name}')>"


class PlayerAlias(Base):
    """
    Player name variations from different sources.

    When we scrape data, player names may be formatted differently:
    - ATP: "Novak Djokovic"
    - Sportsbet: "N. Djokovic"
    - ITF: "DJOKOVIC, Novak"

    This table maps all variations to the canonical player record.
    The source column indicates where this alias came from.
    """
    __tablename__ = "player_aliases"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id", ondelete="CASCADE"))

    # The name variation (stored lowercase for case-insensitive matching)
    alias: Mapped[str] = mapped_column(String(255), nullable=False)

    # Where this alias was found (helps with debugging and priority)
    source: Mapped[str] = mapped_column(String(50), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    player: Mapped["Player"] = relationship(back_populates="aliases")

    # Constraints: same alias from same source shouldn't appear twice
    __table_args__ = (
        UniqueConstraint("alias", "source", name="uq_player_alias_source"),
        Index("idx_player_aliases_alias", "alias"),
    )

    def __repr__(self) -> str:
        return f"<PlayerAlias(alias='{self.alias}', source='{self.source}')>"


class PlayerReviewQueue(Base):
    """
    Queue for player names that couldn't be auto-matched.

    When scraping finds a player name that doesn't match any existing
    player (even with fuzzy matching), it goes into this queue for
    manual review. The system suggests up to 3 possible matches.

    Resolution options:
    - 'matched': Linked to an existing player (adds alias)
    - 'new_player': Created a new player record
    - 'ignored': Skipped (e.g., exhibition match player)
    """
    __tablename__ = "player_review_queue"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The unmatched player data from scraping
    scraped_name: Mapped[str] = mapped_column(String(255), nullable=False)
    scraped_source: Mapped[str] = mapped_column(String(50), nullable=False)
    scraped_external_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Context about where this came from (helps with manual review)
    match_external_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    tournament_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # AI-suggested matches (top 3 candidates with confidence scores)
    # These are computed by the fuzzy matching system
    suggested_player_1_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("players.id"), nullable=True
    )
    suggested_player_1_confidence: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), nullable=True
    )
    suggested_player_2_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("players.id"), nullable=True
    )
    suggested_player_2_confidence: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), nullable=True
    )
    suggested_player_3_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("players.id"), nullable=True
    )
    suggested_player_3_confidence: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), nullable=True
    )

    # Resolution status
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # 'pending', 'matched', 'new_player', 'ignored'
    resolved_player_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("players.id"), nullable=True
    )
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_review_queue_status", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<PlayerReviewQueue(name='{self.scraped_name}', status='{self.status}')>"


# =============================================================================
# Admin Models
# =============================================================================

class AdminUser(Base):
    """Admin user account for protected web workflows."""

    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
    )
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index("idx_admin_users_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<AdminUser(username='{self.username}', active={self.is_active})>"


# =============================================================================
# Tournament Models
# =============================================================================

class Tournament(Base):
    """
    Tournament master data.

    Represents a recurring tournament (e.g., "Australian Open", "Miami Open").
    Each yearly instance is stored in tournament_editions.

    Tournament levels:
    - 'Grand Slam': Australian Open, French Open, Wimbledon, US Open
    - 'Masters 1000' / 'WTA 1000': Top-tier mandatory events
    - 'ATP 500' / 'WTA 500': Mid-tier events
    - 'ATP 250' / 'WTA 250': Lower-tier main tour events
    - 'Challenger': ATP Challenger Tour
    - 'ITF': ITF World Tennis Tour (including former Futures)
    """
    __tablename__ = "tournaments"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Unique code for this tournament (e.g., 'AUSOPEN', 'MIAMI')
    tournament_code: Mapped[str] = mapped_column(String(30), nullable=False)

    # Display name
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Classification
    tour: Mapped[str] = mapped_column(String(15), nullable=False)  # 'ATP', 'WTA', 'Challenger', 'ITF', 'WTA 125'
    gender: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # 'men', 'women'
    level: Mapped[str] = mapped_column(String(30), nullable=False)  # 'Grand Slam', 'Masters 1000', etc.

    # Location
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    country_ioc: Mapped[Optional[str]] = mapped_column(String(3), nullable=True)

    # Playing conditions
    surface: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 'Hard', 'Clay', 'Grass'
    indoor_outdoor: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # 'Indoor', 'Outdoor'

    # External links for scraping
    atp_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    wta_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    itf_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    betting_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    editions: Mapped[list["TournamentEdition"]] = relationship(back_populates="tournament")

    __table_args__ = (
        UniqueConstraint("tournament_code", "tour", "gender", name="uq_tournament_code_tour_gender"),
    )

    def __repr__(self) -> str:
        return f"<Tournament(code='{self.tournament_code}', name='{self.name}')>"


class TournamentEdition(Base):
    """
    A specific year's instance of a tournament.

    Some properties may differ year to year (e.g., surface change,
    prize money), so we store them per edition rather than on
    the tournament master record.
    """
    __tablename__ = "tournament_editions"

    id: Mapped[int] = mapped_column(primary_key=True)
    tournament_id: Mapped[int] = mapped_column(ForeignKey("tournaments.id"))
    year: Mapped[int] = mapped_column(Integer, nullable=False)

    # Dates
    start_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)

    # Edition-specific overrides (if different from tournament defaults)
    surface: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    draw_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    prize_money_usd: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # External IDs for this specific edition
    atp_edition_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    wta_edition_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    itf_edition_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    tournament: Mapped["Tournament"] = relationship(back_populates="editions")
    matches: Mapped[list["Match"]] = relationship(back_populates="tournament_edition")

    __table_args__ = (
        UniqueConstraint("tournament_id", "year", name="uq_tournament_year"),
    )

    def __repr__(self) -> str:
        return f"<TournamentEdition(tournament_id={self.tournament_id}, year={self.year})>"


# =============================================================================
# Match Models
# =============================================================================

class Match(Base):
    """
    Unified match table - handles the full lifecycle from scheduled to completed.

    This table stores ALL matches: scheduled fixtures, in-progress matches, and
    completed results. When a match is first created from a draw, it has status
    'scheduled'. Once played, it's updated with score, winner, and status changes.

    This unified approach avoids the problems of having separate matches/fixtures
    tables where data could get out of sync.

    Score formats:
    - score: Human-readable string like "6-4 3-6 7-6(5)"
    - score_structured: JSONB with parsed sets and tiebreaks

    Match status lifecycle:
    - 'upcoming': Match known from draw (players known, no schedule yet)
    - 'scheduled': Match appears on order of play (has date/time/court)
    - 'in_progress': Match currently being played
    - 'completed': Normal match finish
    - 'retired': Player retired mid-match
    - 'walkover': Player withdrew before match started
    - 'default': Player defaulted (disqualified)
    - 'cancelled': Match cancelled (e.g., weather)
    """
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True)

    # External ID from the source website (unique identifier for deduplication)
    # May be None initially for scheduled matches until we get the official match ID
    external_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, nullable=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # 'atp', 'wta', 'itf'

    # Tournament context
    tournament_edition_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("tournament_editions.id"), nullable=True
    )

    # Round in the tournament
    # Values: 'F', 'SF', 'QF', 'R16', 'R32', 'R64', 'R128', 'Q1', 'Q2', 'Q3', 'RR'
    round: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    match_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Draw position within the round (1-indexed)
    # Used for bracket math: winner of position p feeds into position ceil(p/2) in next round
    # Positions 2p-1 and 2p in round N feed into position p in round N+1
    draw_position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Temporal ordering for chronological comparisons
    # Computed from: match_date + tournament_edition_id + round
    # Use compute_temporal_order() to generate this value
    # Allows simple comparisons: match_a.temporal_order < match_b.temporal_order
    temporal_order: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

    # Players (always use foreign keys, never store names directly)
    player_a_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    player_b_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)

    # Player seeds for this tournament (1 = top seed, None = unseeded)
    # Populated from draw scraping
    player_a_seed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    player_b_seed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # ==========================================================================
    # Scheduling fields (populated when match is first created as a fixture)
    # ==========================================================================

    # When the match is scheduled to be played
    scheduled_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)
    scheduled_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    court: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # ==========================================================================
    # Betting odds (updated as they change before the match)
    # ==========================================================================

    # Decimal odds format (e.g., 1.50 means bet $1 to win $0.50 profit)
    odds_a: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3), nullable=True)
    odds_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3), nullable=True)
    odds_source: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    odds_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # ==========================================================================
    # Model predictions (computed before the match)
    # ==========================================================================

    # Probability that player A wins (0.0000 to 1.0000)
    prediction_a: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    prediction_model_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    prediction_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # ==========================================================================
    # ELO snapshots and processing metadata
    # ==========================================================================

    # Pre-match ELO snapshots (set for upcoming/scheduled and reused at completion)
    elo_pre_player_a: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    elo_pre_player_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Post-match ELO values (set for completed/retired results)
    elo_post_player_a: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    elo_post_player_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # ELO processing metadata
    elo_params_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    elo_processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    elo_needs_recompute: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
    )

    # ==========================================================================
    # Result fields (populated once match is completed)
    # ==========================================================================

    winner_id: Mapped[Optional[int]] = mapped_column(ForeignKey("players.id"), nullable=True)
    score: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Structured score for easier processing
    # Format: [{"a": 6, "b": 4}, {"a": 7, "b": 6, "tb_a": 7, "tb_b": 5}]
    score_structured: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Actual date/time when match was played (may differ from scheduled)
    # If match_date_estimated is True, the date was estimated from tournament dates + round
    match_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)
    match_date_estimated: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    match_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # For retired matches, which set they retired in
    retirement_set: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Detailed match statistics (when available from scraping match details page)
    # Contains serve %, aces, break points, etc.
    stats: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # ==========================================================================
    # Status and metadata
    # ==========================================================================

    # Lifecycle status - see docstring for values
    # Default is 'upcoming' (created from draw, no schedule yet)
    status: Mapped[str] = mapped_column(String(20), default="upcoming")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    tournament_edition: Mapped[Optional["TournamentEdition"]] = relationship(
        back_populates="matches"
    )
    player_a: Mapped["Player"] = relationship(foreign_keys=[player_a_id])
    player_b: Mapped["Player"] = relationship(foreign_keys=[player_b_id])
    winner: Mapped[Optional["Player"]] = relationship(foreign_keys=[winner_id])
    features: Mapped[list["MatchFeatures"]] = relationship(back_populates="match")

    __table_args__ = (
        Index("idx_matches_player_a", "player_a_id"),
        Index("idx_matches_player_b", "player_b_id"),
        Index("idx_matches_scheduled_date", "scheduled_date"),
        Index("idx_matches_match_date", "match_date"),
        Index("idx_matches_tournament", "tournament_edition_id"),
        Index("idx_matches_status", "status"),
        Index("idx_matches_draw", "tournament_edition_id", "round", "draw_position"),
        # Partial index for the ELO incremental query: finds terminal matches
        # that still need ELO processing. In steady state only a handful of
        # matches satisfy this condition, so the index stays tiny and the query
        # avoids a full table scan.
        Index(
            "idx_matches_elo_pending",
            "temporal_order",
            "id",
            postgresql_where=text(
                "status IN ('completed', 'retired', 'walkover', 'default') "
                "AND winner_id IS NOT NULL "
                "AND temporal_order IS NOT NULL "
                "AND (elo_post_player_a IS NULL "
                "OR elo_post_player_b IS NULL "
                "OR elo_needs_recompute = true)"
            ),
        ),
    )

    @property
    def is_completed(self) -> bool:
        """Check if match has finished (any terminal status)."""
        return self.status in ("completed", "retired", "walkover", "default")

    @property
    def is_upcoming(self) -> bool:
        """Check if match is known from draw but not yet scheduled."""
        return self.status == "upcoming"

    @property
    def is_scheduled(self) -> bool:
        """Check if match has a schedule (date/time/court assigned)."""
        return self.status == "scheduled"

    @property
    def is_pending(self) -> bool:
        """Check if match hasn't started yet (either upcoming or scheduled)."""
        return self.status in ("upcoming", "scheduled")

    def update_temporal_order(
        self,
        sibling_date: Optional[datetime] = None,
        tournament_start: Optional[datetime] = None,
        tournament_end: Optional[datetime] = None,
    ) -> None:
        """
        Compute and set the temporal_order field using date fallback chain.

        Fallback chain for date:
        1. match_date - The actual date the match was played
        2. scheduled_date - When the match was scheduled
        3. sibling_date - Date from another match in same tournament/round
        4. Estimated from tournament dates - Based on round and tournament start/end
        5. 9999-12-31 - Last resort fallback (handled in compute_temporal_order)

        Args:
            sibling_date: Optional date from another match in the same tournament/round.
                         Pass this if you've looked up dates from sibling matches.
            tournament_start: Optional tournament start date for estimation.
                             If not provided, tries to get from tournament_edition relationship.
            tournament_end: Optional tournament end date for estimation.
                           If not provided, tries to get from tournament_edition relationship.

        The temporal_order enables simple chronological comparisons:
            if match_a.temporal_order < match_b.temporal_order:
                # match_a happened before match_b
        """
        # Fallback 1 & 2: match_date or scheduled_date
        date_to_use = self.match_date or self.scheduled_date

        # Fallback 3: sibling_date (from same tournament/round)
        if date_to_use is None and sibling_date is not None:
            date_to_use = sibling_date

        # Fallback 4: Estimate from tournament dates
        if date_to_use is None:
            # Try to get tournament dates from relationship or params
            t_start = tournament_start
            t_end = tournament_end

            # If not provided, try to get from the relationship
            if (t_start is None or t_end is None) and self.tournament_edition is not None:
                t_start = t_start or self.tournament_edition.start_date
                t_end = t_end or self.tournament_edition.end_date

            if t_start and t_end:
                date_to_use = estimate_match_date_from_round(
                    round_code=self.round or "R128",
                    tournament_start=t_start,
                    tournament_end=t_end,
                )

        # Compute temporal_order (handles None date with far-future fallback)
        self.temporal_order = compute_temporal_order(
            match_date=date_to_use,
            tournament_edition_id=self.tournament_edition_id or 0,
            round_code=self.round or "R128",
        )

    @classmethod
    def get_round_date_from_siblings(
        cls,
        session,
        tournament_edition_id: int,
        round_code: str,
    ) -> Optional[datetime]:
        """
        Find the most recent match date from sibling matches in the same tournament/round.

        Since a round can span multiple days, we use the most recent date found.
        This allows matches without dates to inherit from matches that do have dates.

        Args:
            session: SQLAlchemy session for querying
            tournament_edition_id: Tournament edition to search in
            round_code: Round to search for (F, SF, QF, etc.)

        Returns:
            Most recent date found, or None if no sibling matches have dates
        """
        from sqlalchemy import func

        result = session.query(func.max(cls.match_date)).filter(
            cls.tournament_edition_id == tournament_edition_id,
            cls.round == round_code,
            cls.match_date.isnot(None),
        ).scalar()

        return result

    def __repr__(self) -> str:
        return f"<Match(id={self.id}, status='{self.status}', external_id='{self.external_id}')>"


class PlayerEloState(Base):
    """Current ELO state per player for incremental inline updates."""

    __tablename__ = "player_elo_states"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False, unique=True)
    rating: Mapped[Decimal] = mapped_column(Numeric(8, 2), nullable=False, default=Decimal("1500.00"))
    match_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_match_date: Mapped[Optional[datetime]] = mapped_column(Date, nullable=True)
    last_temporal_order: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    career_peak: Mapped[Decimal] = mapped_column(Numeric(8, 2), nullable=False, default=Decimal("1500.00"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    player: Mapped["Player"] = relationship()

    def __repr__(self) -> str:
        return f"<PlayerEloState(player_id={self.player_id}, rating={self.rating})>"


class EloParameterSet(Base):
    """Persisted ELO parameter sets (defaults and optimized variants)."""

    __tablename__ = "elo_parameter_sets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    params: Mapped[dict] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_elo_parameter_sets_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<EloParameterSet(name='{self.name}', active={self.is_active})>"


# =============================================================================
# Feature Store Models
# =============================================================================

class FeatureSet(Base):
    """
    Definition of a set of ML features.

    Feature sets are versioned - when you change feature logic, create
    a new version. This allows comparing model performance across
    different feature configurations.

    The feature_definitions JSONB contains the schema for each feature,
    including name, type, and description.
    """
    __tablename__ = "feature_sets"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Unique name for this feature set (e.g., "baseline_v1", "with_serve_stats")
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    version: Mapped[str] = mapped_column(String(20), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Schema defining what features are in this set
    # Format: {"feature_name": {"type": "float", "description": "..."}}
    feature_definitions: Mapped[dict] = mapped_column(JSONB, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    match_features: Mapped[list["MatchFeatures"]] = relationship(back_populates="feature_set")

    def __repr__(self) -> str:
        return f"<FeatureSet(name='{self.name}', version='{self.version}')>"


class MatchFeatures(Base):
    """
    Computed features for a specific match.

    Pre-computing and storing features avoids recalculating them
    every time we need to train or predict. Features are stored
    per feature_set so we can have multiple versions.
    """
    __tablename__ = "match_features"

    id: Mapped[int] = mapped_column(primary_key=True)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"), nullable=False)
    feature_set_id: Mapped[int] = mapped_column(ForeignKey("feature_sets.id"), nullable=False)

    # The computed feature values
    # Format: {"elo_a": 2100.5, "elo_b": 1950.3, "h2h_a_wins": 3, ...}
    features: Mapped[dict] = mapped_column(JSONB, nullable=False)

    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    match: Mapped["Match"] = relationship(back_populates="features")
    feature_set: Mapped["FeatureSet"] = relationship(back_populates="match_features")

    __table_args__ = (
        UniqueConstraint("match_id", "feature_set_id", name="uq_match_features"),
        Index("idx_match_features_match", "match_id"),
        Index("idx_match_features_set", "feature_set_id"),
    )

    def __repr__(self) -> str:
        return f"<MatchFeatures(match_id={self.match_id}, feature_set_id={self.feature_set_id})>"


# =============================================================================
# Operations Models
# =============================================================================

class ScrapeQueue(Base):
    """
    Queue of scraping tasks with retry logic.

    Instead of running scrapes directly and losing data on failures,
    we queue tasks and process them with proper error handling.
    Failed tasks are retried with exponential backoff.

    Task types:
    - 'tournament_results': Scrape completed matches for a tournament
    - 'fixtures': Scrape upcoming matches
    - 'odds': Scrape betting odds
    - 'player_profile': Scrape player details
    - 'historical_tournament': Backfill historical data
    """
    __tablename__ = "scrape_queue"

    id: Mapped[int] = mapped_column(primary_key=True)

    # What to scrape
    task_type: Mapped[str] = mapped_column(String(50), nullable=False)
    task_params: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Priority (1 = highest, 10 = lowest)
    # Current tasks get priority 1-3, historical backfill gets 7-10
    priority: Mapped[int] = mapped_column(Integer, default=5)

    # Status tracking
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # 'pending', 'in_progress', 'completed', 'failed', 'retry'
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)

    # Error tracking
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_scrape_queue_status", "status", "priority", "created_at"),
        CheckConstraint("priority >= 1 AND priority <= 10", name="ck_priority_range"),
    )

    def __repr__(self) -> str:
        return f"<ScrapeQueue(id={self.id}, type='{self.task_type}', status='{self.status}')>"


class UpdateLog(Base):
    """
    Audit log for system updates.

    Records when major operations happen (scraping runs, ELO updates,
    model training, etc.) for debugging and monitoring.
    """
    __tablename__ = "update_log"

    id: Mapped[int] = mapped_column(primary_key=True)

    # What type of update this was
    update_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # Details about the update (varies by type)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Outcome
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_update_log_type_date", "update_type", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<UpdateLog(type='{self.update_type}', success={self.success})>"


class PipelineCheckpoint(Base):
    """Key/value checkpoint store for resumable pipeline stages."""

    __tablename__ = "pipeline_checkpoints"

    key: Mapped[str] = mapped_column(String(120), primary_key=True)
    value_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<PipelineCheckpoint(key='{self.key}')>"


class PipelineRun(Base):
    """Top-level record of an hourly pipeline execution."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    summary_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    stage_runs: Mapped[list["PipelineStageRun"]] = relationship(
        back_populates="pipeline_run",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_pipeline_runs_started_at", "started_at"),
        Index("idx_pipeline_runs_status_started_at", "status", "started_at"),
    )

    def __repr__(self) -> str:
        return f"<PipelineRun(run_id='{self.run_id}', status='{self.status}')>"


class PipelineStageRun(Base):
    """Per-stage execution record for each pipeline run."""

    __tablename__ = "pipeline_stage_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    stage_name: Mapped[str] = mapped_column(String(80), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    metrics_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    pipeline_run: Mapped["PipelineRun"] = relationship(back_populates="stage_runs")

    __table_args__ = (
        Index("idx_pipeline_stage_runs_run_stage", "run_id", "stage_name"),
        Index("idx_pipeline_stage_runs_stage_started_at", "stage_name", "started_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<PipelineStageRun(run_id='{self.run_id}', "
            f"stage='{self.stage_name}', status='{self.status}')>"
        )
