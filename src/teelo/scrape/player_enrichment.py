"""
Player profile enrichment scraper.

Scrapes ATP and WTA player profile pages to extract demographic data:
birth date, height, handedness, backhand style, turned pro year, nationality.

Uses BaseScraper for Playwright browser automation with stealth mode.
Each player gets a fresh page to avoid SPA stale content issues.

Usage:
    async with PlayerEnrichmentScraper(headless=False) as scraper:
        data = await scraper.scrape_atp_profile("a0e2", "carlos-alcaraz")
        data = await scraper.scrape_wta_profile("320760", "aryna-sabalenka")
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture

logger = logging.getLogger(__name__)


@dataclass
class PlayerProfile:
    """Extracted player demographic data from a profile page."""

    birth_date: Optional[date] = None
    height_cm: Optional[int] = None
    hand: Optional[str] = None          # "Right" or "Left"
    backhand: Optional[str] = None      # "One-Handed" or "Two-Handed"
    turned_pro_year: Optional[int] = None
    nationality: Optional[str] = None   # Country name (not IOC code)


class PlayerEnrichmentScraper(BaseScraper):
    """
    Scrapes player profile pages on ATP and WTA websites.

    Both sites are SPAs that ignore the URL slug and only use the numeric/
    alphanumeric player ID. Each player is scraped in a fresh browser page
    to avoid stale content from client-side routing.
    """

    BASE_URL = "https://www.atptour.com"

    # Abstract methods required by BaseScraper (not used for enrichment)
    async def get_tournament_list(self, year: int) -> list[dict]:
        return []

    async def scrape_tournament_results(self, tournament_id: str, year: int) -> AsyncGenerator[ScrapedMatch, None]:
        return
        yield

    async def scrape_fixtures(self, tournament_id: str) -> AsyncGenerator[ScrapedFixture, None]:
        return
        yield

    async def scrape_atp_profile(self, atp_id: str, slug: str = "player") -> Optional[PlayerProfile]:
        """
        Scrape an ATP player profile page.

        ATP profile page structure:
        - div.personal_details contains two columns:
          - ul.pd_left: Age (with DOB as YYYY/MM/DD), Weight, Height, Turned pro
          - ul.pd_right: Country, Birthplace, Plays (with backhand), Coach
        - Each li has two spans: label and value

        Args:
            atp_id: ATP player ID (e.g., "a0e2" for Alcaraz, "D643" for Djokovic)
            slug: URL slug for the player name (ignored by ATP, but included for clean URLs)

        Returns:
            PlayerProfile with extracted data (fields may be None if not found),
            or None if scraping is blocked/fails due to persistent Cloudflare challenge.
        """
        url = f"https://www.atptour.com/en/players/{slug}/{atp_id}/overview"
        logger.info(f"Scraping ATP profile: {url}")

        page = await self.new_page()
        try:
            html = ""
            for attempt in range(1, 4):
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_selector("div.personal_details", timeout=15000)
                except Exception:
                    pass  # will be caught by content check below
                html = await page.content()

                if len(html) >= 5000 and "personal_details" in html:
                    break

                # Cloudflare blocked (page too short or missing content)
                if attempt < 3:
                    wait_secs = 15 * attempt
                    logger.info(
                        f"Cloudflare challenge for {atp_id} (attempt {attempt}), "
                        f"waiting {wait_secs}s..."
                    )
                    await asyncio.sleep(wait_secs)
                else:
                    # Final attempt: reset browser context then fetch
                    logger.info(
                        f"Cloudflare challenge for {atp_id} persists after 2 attempts, "
                        "resetting browser context..."
                    )
                    await self._reset_browser_context()
                    await page.close()
                    page = await self.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        await page.wait_for_selector("div.personal_details", timeout=15000)
                    except Exception:
                        pass  # will be caught by content check below
                    html = await page.content()

            if len(html) < 5000 or "personal_details" not in html:
                logger.warning(
                    f"Cloudflare block persists for {atp_id} after all retries; "
                    "returning None"
                )
                return None

            return self._parse_atp_profile(html)

        finally:
            await page.close()

    async def _reset_browser_context(self) -> None:
        """Close and recreate the browser context to escape a Cloudflare block."""
        if self._context:
            await self._context.close()
        self._context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        self._context.set_default_timeout(self.timeout)
        logger.info("Reset browser context to escape Cloudflare block")

    async def scrape_wta_profile(self, wta_id: str, slug: str = "player") -> PlayerProfile:
        """
        Scrape a WTA player profile page.

        WTA profile page structure:
        - JSON-LD schema.org script with birthDate, birthPlace, nationality
        - div.profile-bio__info-block elements with title/content pairs:
          Plays, Career High, Height, Birthday, Birthplace

        Args:
            wta_id: WTA player ID (numeric, e.g., "320760" for Sabalenka)
            slug: URL slug for the player name (ignored by WTA, but included for clean URLs)

        Returns:
            PlayerProfile with extracted data (fields may be None if not found)
        """
        url = f"https://www.wtatennis.com/players/{wta_id}/{slug}"
        logger.info(f"Scraping WTA profile: {url}")

        page = await self.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

            # Dismiss cookie consent if present
            try:
                btn = await page.query_selector("#onetrust-accept-btn-handler")
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            # Wait for bio section to render
            try:
                await page.wait_for_selector("div.profile-bio__info-block", timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(2)

            html = await page.content()
            return self._parse_wta_profile(html)

        finally:
            await page.close()

    def _parse_atp_profile(self, html: str) -> PlayerProfile:
        """Parse ATP profile HTML into a PlayerProfile."""
        soup = BeautifulSoup(html, "lxml")
        profile = PlayerProfile()

        details = soup.select_one("div.personal_details")
        if not details:
            logger.warning("No personal_details section found in ATP profile")
            return profile

        for li in details.select("li"):
            spans = li.select("span")
            if len(spans) < 2:
                continue

            label = spans[0].get_text(strip=True)
            value = spans[1].get_text(strip=True)

            if label == "Age":
                # "22 (2003/05/05)"
                m = re.search(r'\((\d{4})/(\d{2})/(\d{2})\)', value)
                if m:
                    profile.birth_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

            elif label == "Height":
                # "6'0\" (183cm)"
                m = re.search(r'\((\d+)cm\)', value)
                if m:
                    profile.height_cm = int(m.group(1))

            elif label == "Turned pro":
                m = re.search(r'(\d{4})', value)
                if m:
                    profile.turned_pro_year = int(m.group(1))

            elif label == "Plays":
                # "Right-Handed, Two-Handed Backhand"
                if "right" in value.lower():
                    profile.hand = "Right"
                elif "left" in value.lower():
                    profile.hand = "Left"

                if "two-handed" in value.lower():
                    profile.backhand = "Two-Handed"
                elif "one-handed" in value.lower():
                    profile.backhand = "One-Handed"

            elif label == "Country":
                profile.nationality = value

            elif label == "Birthplace":
                # "Moscow, Russia" or "Minsk, Belarus"
                # Used as fallback for nationality when Country is empty
                # (ATP hides country for Russian and Belarusian players)
                if not profile.nationality and "," in value:
                    country_part = value.split(",")[-1].strip()
                    if country_part:
                        profile.nationality = country_part

        return profile

    def _parse_wta_profile(self, html: str) -> PlayerProfile:
        """Parse WTA profile HTML into a PlayerProfile."""
        soup = BeautifulSoup(html, "lxml")
        profile = PlayerProfile()

        # JSON-LD schema.org data for DOB and nationality
        for script in soup.find_all("script"):
            text = script.string or ""
            if '"@type": "Person"' in text or '"@type":"Person"' in text:
                try:
                    data = json.loads(text.strip())
                    if data.get("birthDate"):
                        parts = data["birthDate"].split("-")
                        profile.birth_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
                    if data.get("nationality"):
                        profile.nationality = data["nationality"]
                except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                    pass
                break

        # Bio info blocks for height and handedness
        for block in soup.select("div.profile-bio__info-block"):
            title_el = block.select_one(".profile-bio__info-title")
            content_el = block.select_one(".profile-bio__info-content")
            if not title_el or not content_el:
                continue

            label = title_el.get_text(strip=True)
            value = content_el.get_text(strip=True)

            if label == "Plays":
                if "right" in value.lower():
                    profile.hand = "Right"
                elif "left" in value.lower():
                    profile.hand = "Left"

            elif label == "Height":
                # "5' 11\" (1.82m)"
                m = re.search(r'\((\d+\.\d+)m\)', value)
                if m:
                    profile.height_cm = int(round(float(m.group(1)) * 100))

        # WTA profile pages don't show backhand style or turned pro year

        return profile
