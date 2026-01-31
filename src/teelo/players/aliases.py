"""
Player name normalization and comparison utilities.

Tennis player names come in many formats from different sources:
- ATP: "Novak Djokovic"
- WTA: "Iga SWIATEK"
- ITF: "DJOKOVIC, Novak"
- Sportsbet: "N. Djokovic"
- With accents: "Rafael Nadal" vs "Rafael Nadál"

This module provides utilities to normalize names for storage and
compare names for fuzzy matching. The goal is to maximize successful
auto-matching while minimizing false positives.
"""

import re
import unicodedata
from typing import Optional

import jellyfish
from rapidfuzz import fuzz


def normalize_name(name: str) -> str:
    """
    Normalize a player name for storage and comparison.

    Normalization steps:
    1. Convert to lowercase
    2. Remove accents (é → e, ñ → n)
    3. Handle "LASTNAME, Firstname" format
    4. Remove extra whitespace
    5. Remove common suffixes (Jr., Sr., III, etc.)

    Args:
        name: Raw player name from any source

    Returns:
        Normalized name suitable for storage in player_aliases table

    Examples:
        >>> normalize_name("Novak DJOKOVIC")
        'novak djokovic'
        >>> normalize_name("SWIATEK, Iga")
        'iga swiatek'
        >>> normalize_name("Carlos Alcaráz")
        'carlos alcaraz'
        >>> normalize_name("Pete Sampras Jr.")
        'pete sampras'
    """
    if not name:
        return ""

    # Step 1: Convert to lowercase
    normalized = name.lower().strip()

    # Step 2: Remove accents using Unicode normalization
    # NFD decomposes characters (é → e + combining acute)
    # Then we filter out the combining characters
    normalized = unicodedata.normalize("NFD", normalized)
    normalized = "".join(
        char for char in normalized
        if unicodedata.category(char) != "Mn"  # Mn = Mark, Nonspacing
    )

    # Step 3: Handle "LASTNAME, Firstname" format (common in ITF)
    if "," in normalized:
        parts = normalized.split(",", 1)
        if len(parts) == 2:
            # Swap to "firstname lastname" format
            normalized = f"{parts[1].strip()} {parts[0].strip()}"

    # Step 4: Remove common suffixes
    suffixes = [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv"]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]

    # Step 5: Clean up whitespace (multiple spaces → single space)
    normalized = " ".join(normalized.split())

    return normalized


def extract_initials(name: str) -> str:
    """
    Extract first letter of each name part.

    Used for matching abbreviated names like "N. Djokovic" to "Novak Djokovic".

    Args:
        name: Normalized player name

    Returns:
        String of initials (e.g., "nd" for "novak djokovic")
    """
    parts = name.split()
    return "".join(part[0] for part in parts if part)


def compare_names(name1: str, name2: str) -> float:
    """
    Compare two player names and return a similarity score.

    Uses multiple comparison algorithms and takes the best score:
    1. Jaro-Winkler: Good for typos and minor variations
    2. Token sort ratio: Handles word order differences
    3. Partial ratio: Handles abbreviations

    The final score is weighted to prefer exact matches while
    still allowing fuzzy matching for reasonable variations.

    Args:
        name1: First name (should be normalized)
        name2: Second name (should be normalized)

    Returns:
        Similarity score from 0.0 (no match) to 1.0 (exact match)

    Examples:
        >>> compare_names("novak djokovic", "novak djokovic")
        1.0
        >>> compare_names("novak djokovic", "n djokovic")
        0.85  # approximate
        >>> compare_names("novak djokovic", "rafael nadal")
        0.45  # approximate
    """
    # Normalize inputs if not already done
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    # Exact match is always 1.0
    if n1 == n2:
        return 1.0

    # Empty strings don't match
    if not n1 or not n2:
        return 0.0

    # Jaro-Winkler similarity (good for typos, gives more weight to prefix matches)
    jw_score = jellyfish.jaro_winkler_similarity(n1, n2)

    # RapidFuzz token sort ratio (handles word order differences)
    # "novak djokovic" vs "djokovic novak" should score high
    token_sort = fuzz.token_sort_ratio(n1, n2) / 100.0

    # RapidFuzz partial ratio (handles abbreviations)
    # "n djokovic" should partially match "novak djokovic"
    partial = fuzz.partial_ratio(n1, n2) / 100.0

    # Special handling for abbreviated first names (common in betting sites)
    # "n. djokovic" vs "novak djokovic"
    abbreviated_bonus = 0.0
    parts1 = n1.split()
    parts2 = n2.split()

    if len(parts1) >= 2 and len(parts2) >= 2:
        # Check if last name matches exactly
        if parts1[-1] == parts2[-1]:
            # Check if first name is abbreviated (single letter or with period)
            first1 = parts1[0].rstrip(".")
            first2 = parts2[0].rstrip(".")

            if len(first1) == 1 and first2.startswith(first1):
                abbreviated_bonus = 0.15
            elif len(first2) == 1 and first1.startswith(first2):
                abbreviated_bonus = 0.15

    # Take the best score from our algorithms
    base_score = max(jw_score, token_sort, partial)

    # Add abbreviated bonus (capped at 1.0)
    final_score = min(1.0, base_score + abbreviated_bonus)

    return final_score


def is_likely_same_player(name1: str, name2: str, threshold: float = 0.90) -> bool:
    """
    Quick check if two names likely refer to the same player.

    This is a convenience wrapper around compare_names for cases
    where you just need a yes/no answer.

    Args:
        name1: First player name
        name2: Second player name
        threshold: Minimum similarity score to consider a match

    Returns:
        True if names are likely the same player
    """
    return compare_names(name1, name2) >= threshold


def extract_last_name(name: str) -> str:
    """
    Extract the last name from a full name.

    Handles common tennis name patterns:
    - "Novak Djokovic" → "djokovic"
    - "Juan Martin del Potro" → "del potro"
    - "Anna-Lena Friedsam" → "friedsam"

    Args:
        name: Full player name (normalized or not)

    Returns:
        Extracted last name (normalized)
    """
    normalized = normalize_name(name)
    parts = normalized.split()

    if not parts:
        return ""

    # Common name particles that are part of last name
    particles = {"de", "del", "van", "von", "da", "di", "la", "le"}

    # If second-to-last word is a particle, include it
    if len(parts) >= 3 and parts[-2] in particles:
        return f"{parts[-2]} {parts[-1]}"

    return parts[-1]


def generate_search_variants(name: str) -> list[str]:
    """
    Generate name variants for searching.

    When looking for a player, we might want to search for multiple
    variations to increase match chances.

    Args:
        name: Player name to generate variants for

    Returns:
        List of name variants to search for

    Examples:
        >>> generate_search_variants("Novak Djokovic")
        ['novak djokovic', 'n djokovic', 'djokovic', 'djokovic novak']
    """
    normalized = normalize_name(name)
    parts = normalized.split()
    variants = [normalized]

    if len(parts) >= 2:
        # First initial + last name
        variants.append(f"{parts[0][0]} {parts[-1]}")

        # Last name only
        variants.append(parts[-1])

        # Reversed order
        variants.append(f"{parts[-1]} {parts[0]}")

        # Last name, First name (ITF format)
        variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")

    return list(set(variants))  # Remove duplicates
