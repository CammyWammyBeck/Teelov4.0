"""
Pytest configuration and fixtures.

This file is automatically loaded by pytest and provides
shared fixtures for all tests.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from teelo.db.models import Base


@pytest.fixture(scope="session")
def test_engine():
    """
    Create a test database engine.

    Uses SQLite in-memory for fast tests that don't need
    PostgreSQL-specific features.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
    )
    return engine


@pytest.fixture(scope="session")
def tables(test_engine):
    """
    Create all tables for testing.

    This fixture runs once per test session.
    """
    Base.metadata.create_all(test_engine)
    yield
    Base.metadata.drop_all(test_engine)


@pytest.fixture
def db_session(test_engine, tables):
    """
    Create a database session for a test.

    Each test gets its own session with automatic rollback,
    ensuring tests don't affect each other.
    """
    connection = test_engine.connect()
    transaction = connection.begin()

    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()
