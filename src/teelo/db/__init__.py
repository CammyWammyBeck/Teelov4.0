"""
Database module for Teelo.

Provides SQLAlchemy ORM models, session management, and common queries.

Usage:
    from teelo.db import get_session, Player, Match

    with get_session() as session:
        players = session.query(Player).all()
"""

from teelo.db.models import (
    Base,
    Player,
    PlayerAlias,
    PlayerReviewQueue,
    Tournament,
    TournamentEdition,
    Match,
    EloRating,
    FeatureSet,
    MatchFeatures,
    ScrapeQueue,
    UpdateLog,
)
from teelo.db.session import get_session, get_engine, SessionLocal

__all__ = [
    # Base
    "Base",
    # Models
    "Player",
    "PlayerAlias",
    "PlayerReviewQueue",
    "Tournament",
    "TournamentEdition",
    "Match",
    "EloRating",
    "FeatureSet",
    "MatchFeatures",
    "ScrapeQueue",
    "UpdateLog",
    # Session
    "get_session",
    "get_engine",
    "SessionLocal",
]
