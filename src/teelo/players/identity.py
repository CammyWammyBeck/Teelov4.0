"""
Player identity service for matching and managing player records.

This is the core service for player identification. It handles:
- Finding existing players by external ID or name
- Creating new player records when needed
- Adding unmatched players to the review queue
- Merging duplicate player records
- Linking cross-tour IDs (ATP player found on ITF)

The matching strategy prioritizes reliability:
1. Exact external ID (ATP/WTA/ITF) - 100% reliable
2. Exact alias match - Very reliable
3. High-confidence fuzzy (>0.98) - Auto-match, add alias
4. Lower confidence - Queue for human review

This approach minimizes false matches while still allowing automation
for clear cases.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from teelo.config import settings
from teelo.db.models import Player, PlayerAlias, PlayerReviewQueue
from teelo.players.aliases import normalize_name, compare_names


@dataclass
class PlayerMatch:
    """
    Result of a player matching attempt.

    Returned by find_player() and related methods to provide
    details about how the match was made.
    """
    player_id: int
    confidence: float  # 0.0 to 1.0
    match_type: str  # 'exact_id', 'exact_alias', 'fuzzy', 'new'
    matched_value: str  # What value was matched (ID or name)

    def __repr__(self) -> str:
        return f"<PlayerMatch(id={self.player_id}, conf={self.confidence:.2f}, type='{self.match_type}')>"


class PlayerIdentityService:
    """
    Service for managing player identity across different data sources.

    This service is the single point of contact for all player matching.
    When scraping data, use this service to convert raw player names
    into canonical player IDs.

    Usage:
        service = PlayerIdentityService(db_session)

        # Find or queue a player
        player_id, status = await service.find_or_queue_player(
            name="Novak Djokovic",
            source="atp",
            external_id="D0AG",
        )

        if status == 'matched':
            # Use player_id
        elif status == 'queued':
            # Player needs manual review
    """

    def __init__(self, db: Session):
        """
        Initialize the identity service.

        Args:
            db: SQLAlchemy session for database operations
        """
        self.db = db

        # Load thresholds from config
        # Above this threshold, auto-match and add alias
        self.exact_match_threshold = settings.player_exact_match_threshold

        # Above this threshold, show as suggestion in review queue
        self.suggestion_threshold = settings.player_suggestion_threshold

    # =========================================================================
    # Main Public Methods
    # =========================================================================

    def find_or_queue_player(
        self,
        name: str,
        source: str,
        external_id: Optional[str] = None,
        match_context: Optional[dict] = None,
    ) -> tuple[Optional[int], str]:
        """
        Find a player or add to review queue if not found.

        This is the main method to use when processing scraped data.
        It attempts to match the player through various strategies and
        either returns a match or queues for review.

        Args:
            name: Player name from the data source
            source: Where this name came from ('atp', 'wta', 'itf', 'sportsbet')
            external_id: Optional external ID (ATP ID, WTA ID, etc.)
            match_context: Optional dict with context for review queue
                          (e.g., {'match_id': '...', 'tournament': '...'})

        Returns:
            Tuple of (player_id, status):
            - (id, 'matched') - Found existing player
            - (id, 'created') - Created new player (high confidence new)
            - (None, 'queued') - Added to review queue

        Examples:
            # With external ID (most reliable)
            player_id, status = service.find_or_queue_player(
                name="Novak Djokovic",
                source="atp",
                external_id="D0AG",
            )

            # Without external ID (uses name matching)
            player_id, status = service.find_or_queue_player(
                name="N. Djokovic",
                source="sportsbet",
                match_context={'tournament': 'Australian Open'},
            )
        """
        # Normalize the name for consistent matching
        normalized_name = normalize_name(name)

        # Strategy 1: Try exact external ID match
        if external_id:
            player = self._find_by_external_id(source, external_id)
            if player:
                # Found by ID - ensure this name variation is stored as alias
                self._ensure_alias(player.id, normalized_name, source)
                return player.id, "matched"

        # Strategy 2: Try exact alias match
        player = self._find_by_exact_alias(normalized_name)
        if player:
            return player.id, "matched"

        # Strategy 3: Try fuzzy matching
        candidates = self._fuzzy_search(normalized_name, limit=3)

        if candidates and candidates[0].confidence >= self.exact_match_threshold:
            # High confidence match - auto-match and add alias
            player_id = candidates[0].player_id
            self._ensure_alias(player_id, normalized_name, source)

            # If we have an external ID, link it to this player
            if external_id:
                self._link_external_id(player_id, source, external_id)

            return player_id, "matched"

        # Strategy 4: Add to review queue
        # No confident match found - needs human review
        self._add_to_review_queue(
            name=name,
            normalized_name=normalized_name,
            source=source,
            external_id=external_id,
            candidates=candidates,
            match_context=match_context,
        )

        return None, "queued"

    def find_player(
        self,
        name: str,
        source: str,
        external_id: Optional[str] = None,
    ) -> Optional[PlayerMatch]:
        """
        Find a player without creating or queuing.

        Use this when you just want to check if a player exists,
        without side effects.

        Args:
            name: Player name to search for
            source: Data source for context
            external_id: Optional external ID

        Returns:
            PlayerMatch if found, None otherwise
        """
        normalized_name = normalize_name(name)

        # Try external ID first
        if external_id:
            player = self._find_by_external_id(source, external_id)
            if player:
                return PlayerMatch(
                    player_id=player.id,
                    confidence=1.0,
                    match_type="exact_id",
                    matched_value=external_id,
                )

        # Try exact alias
        player = self._find_by_exact_alias(normalized_name)
        if player:
            return PlayerMatch(
                player_id=player.id,
                confidence=1.0,
                match_type="exact_alias",
                matched_value=normalized_name,
            )

        # Try fuzzy matching
        candidates = self._fuzzy_search(normalized_name, limit=1)
        if candidates:
            return candidates[0]

        return None

    def create_player(
        self,
        name: str,
        source: str,
        external_id: Optional[str] = None,
        nationality: Optional[str] = None,
    ) -> int:
        """
        Create a new player record.

        Only call this when you're sure this is a new player (e.g., from
        resolving review queue). For normal processing, use find_or_queue_player.

        Args:
            name: Player's canonical name
            source: Where this player was found
            external_id: Optional external ID
            nationality: Optional IOC country code

        Returns:
            ID of the newly created player
        """
        normalized_name = normalize_name(name)

        # Create the player record
        player = Player(
            canonical_name=name,  # Store original casing for display
            nationality_ioc=nationality,
        )

        # Set the appropriate external ID based on source
        if external_id:
            if source == "atp":
                player.atp_id = external_id
            elif source == "wta":
                player.wta_id = external_id
            elif source == "itf":
                player.itf_id = external_id

        self.db.add(player)
        self.db.flush()  # Get the ID without committing

        # Add the normalized name as an alias
        self._ensure_alias(player.id, normalized_name, source)

        self.db.commit()
        return player.id

    def resolve_review_item(
        self,
        review_id: int,
        action: str,
        player_id: Optional[int] = None,
        resolved_by: str = "admin",
    ) -> Optional[int]:
        """
        Resolve a player review queue item.

        Called from the admin interface to handle unmatched players.

        Args:
            review_id: ID of the review queue item
            action: One of 'match', 'create', 'ignore'
            player_id: Required if action is 'match'
            resolved_by: Who resolved this (for audit)

        Returns:
            The player ID (for 'match' and 'create') or None (for 'ignore')

        Raises:
            ValueError: If action is 'match' but no player_id provided
        """
        item = self.db.query(PlayerReviewQueue).filter(
            PlayerReviewQueue.id == review_id
        ).first()

        if not item:
            raise ValueError(f"Review item {review_id} not found")

        normalized_name = normalize_name(item.scraped_name)
        result_player_id = None

        if action == "match":
            # Link to existing player
            if player_id is None:
                raise ValueError("player_id required for 'match' action")

            # Add alias for this name
            self._ensure_alias(player_id, normalized_name, item.scraped_source)

            # Link external ID if we have one
            if item.scraped_external_id:
                self._link_external_id(
                    player_id, item.scraped_source, item.scraped_external_id
                )

            item.resolved_player_id = player_id
            item.status = "matched"
            result_player_id = player_id

        elif action == "create":
            # Create new player
            new_player_id = self.create_player(
                name=item.scraped_name,
                source=item.scraped_source,
                external_id=item.scraped_external_id,
            )
            item.resolved_player_id = new_player_id
            item.status = "new_player"
            result_player_id = new_player_id

        elif action == "ignore":
            # Skip this one (e.g., exhibition player)
            item.status = "ignored"

        else:
            raise ValueError(f"Unknown action: {action}")

        item.resolved_by = resolved_by
        item.resolved_at = datetime.utcnow()
        self.db.commit()

        return result_player_id

    def merge_players(self, keep_id: int, merge_id: int) -> None:
        """
        Merge two player records into one.

        Use this when you discover two records refer to the same person.
        All references (matches, ELO ratings, aliases) are moved to keep_id.

        Args:
            keep_id: Player ID to keep
            merge_id: Player ID to merge (will be deleted)

        Raises:
            ValueError: If either player doesn't exist
        """
        from teelo.db.models import Match, EloRating

        keep_player = self.db.query(Player).filter(Player.id == keep_id).first()
        merge_player = self.db.query(Player).filter(Player.id == merge_id).first()

        if not keep_player:
            raise ValueError(f"Player {keep_id} not found")
        if not merge_player:
            raise ValueError(f"Player {merge_id} not found")

        # Move all matches (both scheduled and completed) where merge_player is player_a
        self.db.query(Match).filter(Match.player_a_id == merge_id).update(
            {"player_a_id": keep_id}
        )

        # Move all matches where merge_player is player_b
        self.db.query(Match).filter(Match.player_b_id == merge_id).update(
            {"player_b_id": keep_id}
        )

        # Move all matches where merge_player is winner
        self.db.query(Match).filter(Match.winner_id == merge_id).update(
            {"winner_id": keep_id}
        )

        # Move aliases (avoid duplicates)
        for alias in merge_player.aliases:
            existing = self.db.query(PlayerAlias).filter(
                PlayerAlias.player_id == keep_id,
                PlayerAlias.alias == alias.alias,
                PlayerAlias.source == alias.source,
            ).first()
            if not existing:
                alias.player_id = keep_id

        # Move ELO ratings
        self.db.query(EloRating).filter(EloRating.player_id == merge_id).update(
            {"player_id": keep_id}
        )

        # Copy external IDs if keep_player doesn't have them
        if merge_player.atp_id and not keep_player.atp_id:
            keep_player.atp_id = merge_player.atp_id
        if merge_player.wta_id and not keep_player.wta_id:
            keep_player.wta_id = merge_player.wta_id
        if merge_player.itf_id and not keep_player.itf_id:
            keep_player.itf_id = merge_player.itf_id

        # Delete the merged player
        self.db.delete(merge_player)
        self.db.commit()

    def link_cross_tour_ids(
        self,
        player_id: int,
        atp_id: Optional[str] = None,
        wta_id: Optional[str] = None,
        itf_id: Optional[str] = None,
    ) -> None:
        """
        Link additional tour IDs to an existing player.

        Use when you discover a player's ID on another tour
        (e.g., ITF player makes ATP debut, we now know their ATP ID).

        Args:
            player_id: Player to update
            atp_id: ATP player ID to link
            wta_id: WTA player ID to link
            itf_id: ITF player ID to link
        """
        player = self.db.query(Player).filter(Player.id == player_id).first()
        if not player:
            raise ValueError(f"Player {player_id} not found")

        if atp_id and not player.atp_id:
            player.atp_id = atp_id
        if wta_id and not player.wta_id:
            player.wta_id = wta_id
        if itf_id and not player.itf_id:
            player.itf_id = itf_id

        self.db.commit()

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _find_by_external_id(self, source: str, external_id: str) -> Optional[Player]:
        """
        Find a player by their external ID from a specific source.

        Args:
            source: 'atp', 'wta', or 'itf'
            external_id: The ID value to search for

        Returns:
            Player if found, None otherwise
        """
        query = self.db.query(Player)

        if source == "atp":
            query = query.filter(Player.atp_id == external_id)
        elif source == "wta":
            query = query.filter(Player.wta_id == external_id)
        elif source == "itf":
            query = query.filter(Player.itf_id == external_id)
        else:
            return None

        return query.first()

    def _find_by_exact_alias(self, normalized_name: str) -> Optional[Player]:
        """
        Find a player by exact alias match.

        Args:
            normalized_name: Normalized name to search for

        Returns:
            Player if found, None otherwise
        """
        alias = self.db.query(PlayerAlias).filter(
            PlayerAlias.alias == normalized_name
        ).first()

        if alias:
            return alias.player

        return None

    def _fuzzy_search(self, normalized_name: str, limit: int = 3) -> list[PlayerMatch]:
        """
        Search for players using fuzzy name matching.

        Gets all players and compares names, returning the best matches.
        This is intentionally simple - for large datasets, consider
        using PostgreSQL's pg_trgm extension for better performance.

        Args:
            normalized_name: Normalized name to search for
            limit: Maximum number of candidates to return

        Returns:
            List of PlayerMatch sorted by confidence (highest first)
        """
        # Get all aliases (this could be optimized with pg_trgm)
        aliases = self.db.query(PlayerAlias).all()

        matches = []
        seen_player_ids = set()

        for alias in aliases:
            # Skip if we've already matched this player
            if alias.player_id in seen_player_ids:
                continue

            # Compare names
            confidence = compare_names(normalized_name, alias.alias)

            # Only consider matches above suggestion threshold
            if confidence >= self.suggestion_threshold:
                matches.append(PlayerMatch(
                    player_id=alias.player_id,
                    confidence=confidence,
                    match_type="fuzzy",
                    matched_value=alias.alias,
                ))
                seen_player_ids.add(alias.player_id)

        # Sort by confidence (highest first) and limit
        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches[:limit]

    def _ensure_alias(self, player_id: int, normalized_name: str, source: str) -> None:
        """
        Ensure an alias exists for a player.

        Creates the alias if it doesn't exist, does nothing if it does.

        Args:
            player_id: Player to add alias for
            normalized_name: Normalized name to add
            source: Source of this alias
        """
        existing = self.db.query(PlayerAlias).filter(
            PlayerAlias.player_id == player_id,
            PlayerAlias.alias == normalized_name,
            PlayerAlias.source == source,
        ).first()

        if not existing:
            alias = PlayerAlias(
                player_id=player_id,
                alias=normalized_name,
                source=source,
            )
            self.db.add(alias)
            # Don't commit here - let caller handle transaction

    def _link_external_id(
        self, player_id: int, source: str, external_id: str
    ) -> None:
        """
        Link an external ID to a player.

        Args:
            player_id: Player to update
            source: Source type ('atp', 'wta', 'itf')
            external_id: External ID to link
        """
        player = self.db.query(Player).filter(Player.id == player_id).first()
        if not player:
            return

        if source == "atp" and not player.atp_id:
            player.atp_id = external_id
        elif source == "wta" and not player.wta_id:
            player.wta_id = external_id
        elif source == "itf" and not player.itf_id:
            player.itf_id = external_id

    def _add_to_review_queue(
        self,
        name: str,
        normalized_name: str,
        source: str,
        external_id: Optional[str],
        candidates: list[PlayerMatch],
        match_context: Optional[dict],
    ) -> None:
        """
        Add an unmatched player to the review queue.

        Args:
            name: Original (unnormalized) name
            normalized_name: Normalized name
            source: Data source
            external_id: Optional external ID
            candidates: Fuzzy match candidates
            match_context: Optional context dict
        """
        item = PlayerReviewQueue(
            scraped_name=name,
            scraped_source=source,
            scraped_external_id=external_id,
            match_external_id=match_context.get("match_id") if match_context else None,
            tournament_name=match_context.get("tournament") if match_context else None,
            status="pending",
        )

        # Add suggestions (up to 3)
        for i, candidate in enumerate(candidates[:3]):
            if candidate.confidence >= self.suggestion_threshold:
                setattr(item, f"suggested_player_{i + 1}_id", candidate.player_id)
                setattr(
                    item,
                    f"suggested_player_{i + 1}_confidence",
                    Decimal(str(round(candidate.confidence, 4))),
                )

        self.db.add(item)
        # Don't commit - let caller handle transaction
