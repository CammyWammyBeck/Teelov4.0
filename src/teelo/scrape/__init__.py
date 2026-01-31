"""
Web scraping module for Teelo.

This module handles all data collection from external sources:
- ATP Tour website (atptour.com)
- WTA Tour website (wtatennis.com)
- ITF Tennis website (itftennis.com)
- Betting odds from Sportsbet and others

Key components:
- BaseScraper: Abstract base class with common scraping logic
- ATPScraper: ATP tour results and fixtures
- WTAScraper: WTA tour results and fixtures
- ITFScraper: ITF circuit results
- ScrapeQueueManager: Queue-based task management with retries

The scraping architecture uses:
- Playwright for browser automation (handles JavaScript-heavy sites)
- BeautifulSoup for HTML parsing
- Async/await for concurrent operations
- Retry logic with exponential backoff for reliability
"""

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
from teelo.scrape.queue import ScrapeQueueManager
from teelo.scrape.atp import ATPScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.wta import WTAScraper

__all__ = [
    "BaseScraper",
    "ScrapedMatch",
    "ScrapedFixture",
    "ScrapeQueueManager",
    "ATPScraper",
    "ITFScraper",
    "WTAScraper",
]
