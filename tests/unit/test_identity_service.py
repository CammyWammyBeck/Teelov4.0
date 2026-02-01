"""
Unit tests for PlayerIdentityService.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from teelo.db.models import Base, Player, PlayerAlias
from teelo.players.identity import PlayerIdentityService
from teelo.config import settings

@pytest.fixture
def db_session():
    """Create a clean in-memory database for each test."""
    engine = create_engine("sqlite:///:memory:")
    # Only create the tables we need for these tests to avoid JSONB issues with SQLite
    Player.__table__.create(engine)
    PlayerAlias.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

def test_find_by_exact_alias_pending(db_session):
    """Test that _find_by_exact_alias finds aliases pending in the session."""
    service = PlayerIdentityService(db_session)
    
    # Create a player and add an alias to the session without committing
    player = Player(canonical_name="Diego Dedura-Palomero")
    db_session.add(player)
    db_session.flush()
    
    alias = PlayerAlias(player_id=player.id, alias="diego dedura-palomero", source="itf")
    db_session.add(alias)
    
    # Verify it can be found
    found_player = service._find_by_exact_alias("diego dedura-palomero")
    assert found_player is not None
    assert found_player.id == player.id

def test_ensure_alias_pending_duplicate(db_session):
    """Test that _ensure_alias doesn't add duplicate aliases even if pending in session."""
    service = PlayerIdentityService(db_session)
    
    player = Player(canonical_name="Diego Dedura-Palomero")
    db_session.add(player)
    db_session.flush()
    
    # Add alias once
    service._ensure_alias(player.id, "diego dedura-palomero", "itf")
    
    # Try adding again (different player ID, same alias/source)
    # This should be prevented by _ensure_alias to avoid UniqueViolation
    player2 = Player(canonical_name="Another Player")
    db_session.add(player2)
    db_session.flush()
    
    service._ensure_alias(player2.id, "diego dedura-palomero", "itf")
    
    # Check how many aliases are in database (including flushed)
    all_aliases = db_session.query(PlayerAlias).all()
    assert len(all_aliases) == 1
    assert all_aliases[0].player_id == player.id

def test_find_or_queue_player_batch_consistency(db_session):
    """Test consistency of find_or_queue_player when called multiple times in a batch."""
    service = PlayerIdentityService(db_session)
    
    # Mock settings if needed, but defaults should be fine
    
    # 1. Create a player via create_player (which commits)
    player_id = service.create_player("Diego Dedura-Palomero", "itf", external_id="12345")
    
    # 2. Call find_or_queue_player for the same player
    # This should match via exact alias (committed)
    pid1, status1 = service.find_or_queue_player("Diego Dedura-Palomero", "itf", external_id="12345")
    assert pid1 == player_id
    assert status1 == "matched"
    
    # 3. Call for the same name but NO external ID
    # This should match via exact alias (committed)
    pid2, status2 = service.find_or_queue_player("Diego Dedura-Palomero", "itf")
    assert pid2 == player_id
    assert status2 == "matched"

def test_fuzzy_match_sees_pending_aliases(db_session):
    """Test that fuzzy search considers aliases pending in the session."""
    service = PlayerIdentityService(db_session)
    service.exact_match_threshold = 0.95
    service.suggestion_threshold = 0.80
    
    # Add a player and alias to session
    player = Player(canonical_name="Novak Djokovic")
    db_session.add(player)
    db_session.flush()
    
    alias = PlayerAlias(player_id=player.id, alias="novak djokovic", source="atp")
    db_session.add(alias)
    
    # Search for a similar name
    candidates = service._fuzzy_search("n. djokovic")
    assert len(candidates) > 0
    assert candidates[0].player_id == player.id
