"""
Database session management for Teelo.

Provides SQLAlchemy engine and session factory with proper
connection pooling configuration. Uses the settings from config.py.

Usage:
    # As a context manager (recommended for scripts)
    from teelo.db import get_session

    with get_session() as session:
        players = session.query(Player).all()
        session.add(new_player)
        # Commits automatically on exit, rolls back on exception

    # As a dependency injection (for FastAPI)
    from teelo.db import SessionLocal

    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from teelo.config import settings


def get_engine():
    """
    Create SQLAlchemy engine with connection pooling.

    The engine is configured with:
    - Connection pool for efficient reuse
    - Echo mode disabled (set LOG_LEVEL=DEBUG for SQL logging)
    - Pre-ping to verify connections before use (handles stale connections)
    """
    engine = create_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,  # Verify connection is alive before using
        echo=settings.log_level == "DEBUG",  # Log SQL only in debug mode
    )

    # Log connection events for debugging
    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, connection_record):
        """Called when a new connection is created."""
        # Enable auto-commit for some operations if needed
        pass

    return engine


# Create the engine (singleton pattern via module-level variable)
_engine = None


def _get_engine():
    """Get or create the singleton engine instance."""
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


# Session factory - creates new sessions bound to our engine
SessionLocal = sessionmaker(
    autocommit=False,  # We'll handle commits explicitly
    autoflush=False,  # Don't auto-flush before queries (more control)
    bind=_get_engine(),
)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Automatically commits on successful exit, rolls back on exception.
    This is the recommended way to use sessions in scripts and tasks.

    Example:
        with get_session() as session:
            player = session.query(Player).filter_by(name="Djokovic").first()
            player.elo = 2500
            # Commits automatically when exiting the block

    Raises:
        Any exception from the database operation (after rollback)
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """
    Dependency injection function for FastAPI.

    Use this with FastAPI's Depends() for request-scoped sessions.

    Example:
        @router.get("/players")
        def list_players(db: Session = Depends(get_db)):
            return db.query(Player).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
