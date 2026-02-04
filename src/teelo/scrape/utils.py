"""
Shared utilities for scraping tasks.
"""

from teelo.scrape.atp import ATPScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.wta import WTAScraper

# Tour types configuration
TOUR_TYPES = {
    "ATP": {"scraper": "atp", "tour_type": "main", "description": "ATP Main Tour"},
    "CHALLENGER": {"scraper": "atp", "tour_type": "challenger", "description": "ATP Challenger Tour"},
    "ITF_MEN": {"scraper": "itf", "gender": "men", "description": "ITF Men's World Tennis Tour"},
    "ITF_WOMEN": {"scraper": "itf", "gender": "women", "description": "ITF Women's World Tennis Tour"},
    "WTA": {"scraper": "wta", "tour_type": "main", "description": "WTA Tour"},
    "WTA_125": {"scraper": "wta", "tour_type": "125", "description": "WTA 125 Tour"},
}

async def get_tournaments_for_tour(tour_key: str, year: int) -> list[dict]:
    """
    Fetch tournament list for a specific tour and year using the appropriate scraper.

    Args:
        tour_key: Tour type key (ATP, CHALLENGER, ITF_MEN, etc.)
        year: Year to get tournaments for

    Returns:
        List of tournament dictionaries
    """
    tour_config = TOUR_TYPES.get(tour_key)
    if not tour_config:
        print(f"Warning: Unknown tour key {tour_key}")
        return []

    if tour_config["scraper"] == "atp":
        # ATP scraper handles headless config internally based on settings,
        # but here we force headless=False for better reliability/stealth if needed,
        # or stick to the class default. Backfill used headless=False.
        async with ATPScraper(headless=False) as scraper:
            return await scraper.get_tournament_list(
                year,
                tour_type=tour_config["tour_type"],
            )

    elif tour_config["scraper"] == "itf":
        async with ITFScraper(headless=False) as scraper:
            return await scraper.get_tournament_list(
                year,
                gender=tour_config["gender"],
            )

    elif tour_config["scraper"] == "wta":
        async with WTAScraper(headless=False) as scraper:
            return await scraper.get_tournament_list(
                year,
                tour_type=tour_config.get("tour_type", "main"),
            )

    else:
        return []
