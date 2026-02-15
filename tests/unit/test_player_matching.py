"""
Unit tests for player name matching.

Tests the name normalization and fuzzy matching logic that's
critical for correctly identifying players across data sources.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from teelo.players.aliases import (
    compare_names,
    extract_last_name,
    generate_search_variants,
    is_abbreviated_name,
    is_likely_same_player,
    normalize_name,
)
from teelo.players.identity import PlayerIdentityService
from teelo.db.models import Player, PlayerAlias, PlayerReviewQueue
from teelo.scrape.parsers.player import extract_seed_from_name


class TestNormalizeName:
    """Tests for name normalization."""

    def test_lowercase(self):
        """Test that names are lowercased."""
        assert normalize_name("Novak DJOKOVIC") == "novak djokovic"

    def test_remove_accents(self):
        """Test accent removal."""
        assert normalize_name("Carlos Alcaráz") == "carlos alcaraz"
        assert normalize_name("Garbiñe Muguruza") == "garbine muguruza"
        assert normalize_name("Jiří Veselý") == "jiri vesely"

    def test_itf_format(self):
        """Test handling of ITF LASTNAME, Firstname format."""
        assert normalize_name("DJOKOVIC, Novak") == "novak djokovic"
        assert normalize_name("SWIATEK, Iga") == "iga swiatek"

    def test_remove_suffixes(self):
        """Test removal of Jr., Sr., etc."""
        assert normalize_name("Pete Sampras Jr.") == "pete sampras"
        assert normalize_name("John McEnroe Sr") == "john mcenroe"

    def test_whitespace_cleanup(self):
        """Test multiple spaces are collapsed."""
        assert normalize_name("Novak    Djokovic") == "novak djokovic"
        assert normalize_name("  Novak Djokovic  ") == "novak djokovic"

    def test_empty_string(self):
        """Test empty string handling."""
        assert normalize_name("") == ""
        assert normalize_name("   ") == ""


class TestCompareNames:
    """Tests for name comparison."""

    def test_exact_match(self):
        """Test exact matches return 1.0."""
        score = compare_names("novak djokovic", "novak djokovic")
        assert score == 1.0

    def test_case_insensitive(self):
        """Test comparison is case insensitive."""
        score = compare_names("Novak Djokovic", "novak djokovic")
        assert score == 1.0

    def test_abbreviated_first_name(self):
        """Test matching abbreviated first names (common in betting)."""
        score = compare_names("novak djokovic", "n djokovic")
        # Should be high but not perfect
        assert score > 0.85

    def test_completely_different(self):
        """Test completely different names have low score."""
        score = compare_names("novak djokovic", "rafael nadal")
        assert score < 0.6

    def test_same_last_name_different_first(self):
        """Test same last name but different first."""
        score = compare_names("novak djokovic", "marko djokovic")
        # Same last name should give reasonable score
        assert 0.6 < score < 0.95

    def test_word_order_difference(self):
        """Test reversed word order."""
        score = compare_names("novak djokovic", "djokovic novak")
        assert score > 0.8  # Token sort should handle this

    def test_empty_strings(self):
        """Test empty string handling."""
        assert compare_names("", "") == 0.0
        assert compare_names("novak", "") == 0.0


class TestIsLikelySamePlayer:
    """Tests for the convenience function."""

    def test_same_player(self):
        """Test obvious same player."""
        assert is_likely_same_player("Novak Djokovic", "Novak Djokovic")

    def test_different_player(self):
        """Test obviously different players."""
        assert not is_likely_same_player("Novak Djokovic", "Rafael Nadal")

    def test_custom_threshold(self):
        """Test with custom threshold."""
        # With high threshold, abbreviated name shouldn't match
        assert not is_likely_same_player(
            "Novak Djokovic", "N. Djokovic", threshold=0.99
        )

        # With lower threshold, it should
        assert is_likely_same_player(
            "Novak Djokovic", "N. Djokovic", threshold=0.80
        )


class TestExtractLastName:
    """Tests for last name extraction."""

    def test_simple_name(self):
        """Test simple two-part name."""
        assert extract_last_name("Novak Djokovic") == "djokovic"

    def test_name_with_particle(self):
        """Test names with particles like 'del', 'de', 'van'."""
        assert extract_last_name("Juan Martin del Potro") == "del potro"

    def test_hyphenated_name(self):
        """Test hyphenated names."""
        assert extract_last_name("Anna-Lena Friedsam") == "friedsam"

    def test_single_name(self):
        """Test single name (edge case)."""
        assert extract_last_name("Madonna") == "madonna"


class TestIsAbbreviatedName:
    """Tests for abbreviated-name detection."""

    def test_abbreviated(self):
        assert is_abbreviated_name("J. Pegula")
        assert is_abbreviated_name("J Pegula")

    def test_not_abbreviated(self):
        assert not is_abbreviated_name("Jessica Pegula")
        assert not is_abbreviated_name("Pegula")


class TestExtractSeedFromName:
    """Tests for extracting seed numbers from names."""

    def test_prefix_parentheses(self):
        """Test (1) prefix format."""
        name, seed = extract_seed_from_name("(1) Novak Djokovic")
        assert name == "Novak Djokovic"
        assert seed == 1

    def test_prefix_brackets(self):
        """Test [1] prefix format."""
        name, seed = extract_seed_from_name("[2] Carlos Alcaraz")
        assert name == "Carlos Alcaraz"
        assert seed == 2

    def test_suffix_seed(self):
        """Test suffix format."""
        name, seed = extract_seed_from_name("Novak Djokovic (1)")
        assert name == "Novak Djokovic"
        assert seed == 1

    def test_no_seed(self):
        """Test name without seed."""
        name, seed = extract_seed_from_name("Novak Djokovic")
        assert name == "Novak Djokovic"
        assert seed is None

    def test_wildcard_qualifier(self):
        """Test WC/Q qualifier removal."""
        name, seed = extract_seed_from_name("John Doe [WC]")
        assert name == "John Doe"
        assert seed is None

        name, seed = extract_seed_from_name("Jane Doe (Q)")
        assert name == "Jane Doe"
        assert seed is None


class TestGenerateSearchVariants:
    """Tests for search variant generation."""

    def test_basic_variants(self):
        """Test basic variant generation."""
        variants = generate_search_variants("Novak Djokovic")

        assert "novak djokovic" in variants
        assert "n djokovic" in variants
        assert "djokovic" in variants

    def test_no_duplicates(self):
        """Test no duplicate variants."""
        variants = generate_search_variants("Novak Djokovic")
        assert len(variants) == len(set(variants))

    def test_single_name(self):
        """Test single name handling."""
        variants = generate_search_variants("Cher")
        assert "cher" in variants


@pytest.fixture
def identity_db_session():
    """Create a sqlite DB with identity tables used by matching tests."""
    engine = create_engine("sqlite:///:memory:")
    Player.__table__.create(engine)
    PlayerAlias.__table__.create(engine)
    PlayerReviewQueue.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


class TestIdentityAbbreviatedMatching:
    """Identity service tests for abbreviated cross-source names."""

    def test_find_by_abbreviated_match(self, identity_db_session):
        service = PlayerIdentityService(identity_db_session)
        player_id = service.create_player("Juan Martin del Potro", "itf", external_id="ITF-DP")
        matched = service._find_by_abbreviated_match(normalize_name("J. del Potro"))
        assert matched is not None
        assert matched.id == player_id

    def test_find_or_queue_player_abbreviated_cross_source(self, identity_db_session):
        service = PlayerIdentityService(identity_db_session)
        existing_player_id = service.create_player("Jessica Pegula", "itf", external_id="ITF-PEGULA")

        matched_player_id, status = service.find_or_queue_player(
            name="J. Pegula",
            source="wta",
            external_id="WTA-PEGULA",
        )

        assert status == "matched"
        assert matched_player_id == existing_player_id

        player = identity_db_session.get(Player, existing_player_id)
        assert player is not None
        assert player.wta_id == "WTA-PEGULA"

    def test_ambiguous_abbreviated_name_queues_review(self, identity_db_session):
        service = PlayerIdentityService(identity_db_session)
        p1 = service.create_player("Karolina Pliskova", "itf", external_id="ITF-KARO")
        p2 = service.create_player("Kristyna Pliskova", "itf", external_id="ITF-KRIS")

        player_id, status = service.find_or_queue_player(
            name="K. Pliskova",
            source="wta",
            external_id="WTA-PLISKOVA",
        )

        assert player_id is None
        assert status == "queued"

        queued = identity_db_session.query(PlayerReviewQueue).first()
        assert queued is not None
        suggested_ids = {
            queued.suggested_player_1_id,
            queued.suggested_player_2_id,
            queued.suggested_player_3_id,
        }
        assert p1 in suggested_ids
        assert p2 in suggested_ids

    def test_hybrid_fuzzy_fallback_with_abbreviation(self, identity_db_session, monkeypatch):
        service = PlayerIdentityService(identity_db_session)
        service.exact_match_threshold = 0.90
        service.suggestion_threshold = 0.30
        player_id = service.create_player("Jessica Pegula", "itf", external_id="ITF-PEG")

        class _NoRows:
            def fetchall(self):
                return []

        monkeypatch.setattr(service, "_trigram_enabled", lambda: True)
        original_execute = identity_db_session.execute

        def _execute_with_trigram_mock(*args, **kwargs):
            statement = args[0] if args else None
            if statement is not None and "similarity(alias, :name)" in str(statement):
                return _NoRows()
            return original_execute(*args, **kwargs)

        monkeypatch.setattr(identity_db_session, "execute", _execute_with_trigram_mock)

        candidates = service._fuzzy_search(normalize_name("J. Pegula"), limit=3)
        assert candidates
        assert candidates[0].player_id == player_id
