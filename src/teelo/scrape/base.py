"""
Base scraper class and common data structures.

Provides the foundation for all tour-specific scrapers (ATP, WTA, ITF).
Uses Playwright for browser automation to handle JavaScript-rendered content.

Key features:
- Async context manager for proper resource cleanup
- Retry logic with exponential backoff
- Random delays to avoid rate limiting
- Stealth mode to avoid bot detection (Cloudflare, etc.)
- Standardized data structures for match data
"""

import asyncio
import logging
import os
import random
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncGenerator, Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth

from teelo.config import settings

logger = logging.getLogger(__name__)

# Stealth configuration to avoid bot detection (Cloudflare, etc.)
_stealth = Stealth()


@dataclass
class ScrapedMatch:
    """
    Standardized match data from any tour source.

    This dataclass normalizes match data across different sources
    (ATP, WTA, ITF) into a common format for processing.

    All fields use strings to avoid type conversion issues during
    scraping - type conversion happens during database insertion.
    """

    # Required fields (no defaults) - must come first in dataclass
    # ============================================================

    # Unique identifier from the source website
    external_id: str

    # Where this data came from ('atp', 'wta', 'itf')
    source: str

    # Tournament information
    tournament_name: str
    tournament_id: str
    tournament_year: int
    tournament_level: str  # 'Grand Slam', 'Masters 1000', 'ATP 500', etc.
    tournament_surface: str  # 'Hard', 'Clay', 'Grass', 'Carpet'

    # Match context
    round: str  # 'F', 'SF', 'QF', 'R16', 'R32', 'R64', 'R128', 'Q1', 'Q2', 'Q3', 'RR'

    # Optional fields (with defaults) - must come after required fields
    # ==================================================================

    tournament_location: Optional[str] = None
    tournament_country_ioc: Optional[str] = None

    match_number: Optional[int] = None

    # Player A (typically the winner in completed matches)
    player_a_name: str = ""
    player_a_external_id: Optional[str] = None
    player_a_nationality: Optional[str] = None
    player_a_seed: Optional[int] = None

    # Player B (typically the loser in completed matches)
    player_b_name: str = ""
    player_b_external_id: Optional[str] = None
    player_b_nationality: Optional[str] = None
    player_b_seed: Optional[int] = None

    # Result
    winner_name: Optional[str] = None
    score_raw: str = ""  # e.g., "6-4 3-6 7-6(5)"

    # Timing
    match_date: Optional[str] = None  # ISO format: "2024-01-15"
    match_time: Optional[str] = None  # HH:MM format
    duration_minutes: Optional[int] = None

    # Match completion status
    status: str = "completed"  # 'completed', 'retired', 'walkover', 'default'
    retirement_set: Optional[int] = None  # Which set player retired in

    # Detailed statistics (optional - may not be available)
    stats: Optional[dict] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"<ScrapedMatch({self.player_a_name} vs {self.player_b_name}, "
            f"{self.tournament_name} {self.round})>"
        )


@dataclass
class ScrapedFixture:
    """
    Standardized upcoming match (fixture) data.

    Similar to ScrapedMatch but for matches that haven't been played yet.
    Used for prediction and betting odds analysis.
    """

    # Required fields (no defaults) - must come first
    # ================================================

    # Tournament information
    tournament_name: str
    tournament_id: str
    tournament_year: int
    tournament_level: str
    tournament_surface: str

    # Match context
    round: str

    # Optional fields (with defaults) - must come after required fields
    # ==================================================================

    tournament_location: Optional[str] = None

    scheduled_date: Optional[str] = None  # ISO format
    scheduled_time: Optional[str] = None  # HH:MM format
    court: Optional[str] = None

    # Players
    player_a_name: str = ""
    player_a_external_id: Optional[str] = None
    player_a_seed: Optional[int] = None

    player_b_name: str = ""
    player_b_external_id: Optional[str] = None
    player_b_seed: Optional[int] = None

    # Where this data came from
    source: str = "atp"

    def __repr__(self) -> str:
        return (
            f"<ScrapedFixture({self.player_a_name} vs {self.player_b_name}, "
            f"{self.tournament_name} {self.round})>"
        )


@dataclass
class ScrapedDrawEntry:
    """
    A single entry from a tournament draw bracket.

    Represents one match slot in the draw, which may be completed (with score
    and winner), upcoming (both players known), or TBD (one/both players unknown).
    Byes are also represented as draw entries with is_bye=True.

    Draw positions are 1-indexed within each round:
    - R128: positions 1-64 (64 matches)
    - R64: positions 1-32
    - ...
    - F: position 1

    The positional math for bracket progression:
    - Winner of position p feeds into position ceil(p/2) in the next round
    - Positions 2p-1 and 2p are the two feeder matches for position p
    """

    # Match context within the draw
    round: str                              # Normalized: 'R128', 'R64', ..., 'F'
    draw_position: int                      # 1-indexed position within the round

    # Player A (top of the draw slot)
    player_a_name: Optional[str] = None
    player_a_external_id: Optional[str] = None
    player_a_seed: Optional[int] = None

    # Player B (bottom of the draw slot)
    player_b_name: Optional[str] = None
    player_b_external_id: Optional[str] = None
    player_b_seed: Optional[int] = None

    # Result (None if not yet played)
    score_raw: Optional[str] = None
    winner_name: Optional[str] = None

    # Special cases
    is_bye: bool = False

    # Tournament context (populated by the scraper)
    source: str = "atp"
    tournament_name: str = ""
    tournament_id: str = ""
    tournament_year: int = 0
    tournament_level: str = ""
    tournament_surface: str = ""

    def __repr__(self) -> str:
        p_a = self.player_a_name or "TBD"
        p_b = self.player_b_name or "TBD"
        return f"<ScrapedDrawEntry({self.round} #{self.draw_position}: {p_a} vs {p_b})>"


class VirtualDisplay:
    """
    Manages an Xvfb virtual display with optional x11vnc and noVNC servers.

    This allows running a headed browser (headless=False) on machines without
    a physical display (e.g., headless servers). The browser renders into
    a virtual framebuffer, and you can optionally view it via:
    - VNC client on port 5900 (configurable)
    - Web browser at http://host:6080/vnc.html (noVNC)

    Uses a singleton pattern - only one virtual display runs per process.
    The display stays alive for the entire process lifetime so VNC connections
    aren't interrupted between scraper tasks. Cleaned up automatically on
    exit (including Ctrl+C).

    Usage:
        # At script startup:
        VirtualDisplay.ensure_running()

        # ... run scrapers (they call acquire/release internally) ...

        # At script end (or on Ctrl+C, handled automatically):
        VirtualDisplay.shutdown()
    """

    _instance: Optional["VirtualDisplay"] = None

    def __init__(self, display_num: int = 99):
        self.display_num = display_num
        self.display = f":{display_num}"
        self._xvfb_proc: Optional[subprocess.Popen] = None
        self._vnc_proc: Optional[subprocess.Popen] = None
        self._novnc_proc: Optional[subprocess.Popen] = None
        self._original_wayland_display: Optional[str] = None
        self._running = False

    @classmethod
    def ensure_running(cls) -> "VirtualDisplay":
        """
        Start the virtual display if not already running.

        Safe to call multiple times - only starts once. Registers atexit
        and signal handlers for graceful cleanup on Ctrl+C or process exit.
        """
        if cls._instance is None or not cls._instance._running:
            cls._instance = VirtualDisplay()
            cls._instance.start()
            cls._instance._register_cleanup()
        return cls._instance

    @classmethod
    def acquire(cls) -> "VirtualDisplay":
        """Get or create the singleton virtual display (called by BaseScraper)."""
        return cls.ensure_running()

    @classmethod
    def release(cls) -> None:
        """No-op - display stays alive until explicit shutdown or process exit."""
        pass

    @classmethod
    def shutdown(cls) -> None:
        """Explicitly stop the virtual display and all associated processes."""
        if cls._instance is not None and cls._instance._running:
            cls._instance.stop()
            cls._instance = None

    def start(self) -> None:
        """Start Xvfb, x11vnc, and noVNC if available."""
        # Start Xvfb (virtual framebuffer)
        xvfb_path = shutil.which("Xvfb")
        if not xvfb_path:
            raise RuntimeError(
                "Xvfb not found. Install it:\n"
                "  Ubuntu/Debian: sudo apt install xvfb\n"
                "  Arch: sudo pacman -S xorg-server-xvfb"
            )

        self._xvfb_proc = subprocess.Popen(
            ["Xvfb", self.display, "-screen", "0", "1920x1080x24", "-ac"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Force Chromium to use X11 instead of Wayland, and point it at our
        # virtual display. Without this, Chromium on Wayland ignores DISPLAY
        # and renders on the real screen.
        os.environ["DISPLAY"] = self.display
        self._original_wayland_display = os.environ.pop("WAYLAND_DISPLAY", None)
        os.environ["XDG_SESSION_TYPE"] = "x11"
        logger.info("Xvfb started on display %s (PID %d)", self.display, self._xvfb_proc.pid)

        # Start x11vnc if available (allows VNC clients to connect)
        vnc_path = shutil.which("x11vnc")
        if vnc_path:
            vnc_bind_args = ["-localhost"]
            if settings.scrape_vnc_bind not in ("127.0.0.1", "localhost"):
                vnc_bind_args = ["-listen", settings.scrape_vnc_bind]
            vnc_auth_args = ["-nopw"]
            if settings.scrape_vnc_password:
                vnc_auth_args = ["-passwd", settings.scrape_vnc_password]
            self._vnc_proc = subprocess.Popen(
                [
                    "x11vnc", "-display", self.display,
                    "-forever", "-shared",
                    *vnc_auth_args,
                    *vnc_bind_args,
                    "-rfbport", str(settings.scrape_vnc_port),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(
                "x11vnc started on port %d (PID %d)",
                settings.scrape_vnc_port, self._vnc_proc.pid,
            )
        else:
            logger.warning("x11vnc not found - VNC viewing disabled")

        # Start noVNC if available (browser-based VNC viewer)
        novnc_path = shutil.which("novnc")
        # noVNC can also be launched via websockify or the launch.sh script
        if not novnc_path:
            novnc_path = shutil.which("websockify")
        if novnc_path and self._vnc_proc:
            self._novnc_proc = subprocess.Popen(
                [
                    "websockify", "--web", "/usr/share/novnc",
                    f"{settings.scrape_novnc_bind}:{settings.scrape_novnc_port}",
                    f"localhost:{settings.scrape_vnc_port}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info(
                "noVNC started - view at http://localhost:%d/vnc.html (PID %d)",
                settings.scrape_novnc_port, self._novnc_proc.pid,
            )
        elif not novnc_path:
            logger.warning("noVNC/websockify not found - browser viewing disabled")

        self._running = True

    def _register_cleanup(self) -> None:
        """Register atexit and signal handlers for graceful shutdown."""
        import atexit
        import signal

        atexit.register(VirtualDisplay.shutdown)

        # Handle Ctrl+C and SIGTERM gracefully
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def _cleanup_handler(signum, frame):
            """Stop the virtual display, then call the original handler."""
            VirtualDisplay.shutdown()
            # Re-raise with the original handler so the process exits normally
            handler = original_sigint if signum == signal.SIGINT else original_sigterm
            if callable(handler):
                handler(signum, frame)
            elif handler == signal.SIG_DFL:
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

        signal.signal(signal.SIGINT, _cleanup_handler)
        signal.signal(signal.SIGTERM, _cleanup_handler)

    def stop(self) -> None:
        """Stop all virtual display processes."""
        if not self._running:
            return
        self._running = False

        for name, proc in [
            ("noVNC", self._novnc_proc),
            ("x11vnc", self._vnc_proc),
            ("Xvfb", self._xvfb_proc),
        ]:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                logger.info("%s stopped (PID %d)", name, proc.pid)

        # Restore original display environment
        if os.environ.get("DISPLAY") == self.display:
            os.environ.pop("DISPLAY", None)
        if self._original_wayland_display:
            os.environ["WAYLAND_DISPLAY"] = self._original_wayland_display
            os.environ["XDG_SESSION_TYPE"] = "wayland"


class BaseScraper(ABC):
    """
    Abstract base class for all tennis data scrapers.

    Provides common functionality:
    - Playwright browser management (async context manager)
    - Page navigation with retry logic
    - Random delays to avoid rate limiting
    - Logging and error handling

    Subclasses must implement:
    - scrape_tournament_results(): Scrape completed matches
    - scrape_fixtures(): Scrape upcoming matches
    - get_tournament_list(): Get list of tournaments for a year

    Usage:
        async with ATPScraper() as scraper:
            tournaments = await scraper.get_tournament_list(2024)
            for tournament in tournaments:
                async for match in scraper.scrape_tournament_results(tournament, 2024):
                    process_match(match)
    """

    # Base URLs for different tours (override in subclasses)
    BASE_URL: str = ""

    def __init__(self, headless: bool = None):
        """
        Initialize the scraper.

        Args:
            headless: Whether to run browser in headless mode.
                     If None, uses settings.scrape_headless
        """
        self.headless = headless if headless is not None else settings.scrape_headless
        self.timeout = settings.scrape_timeout
        self._use_virtual_display = settings.scrape_virtual_display and not self.headless

        # Playwright objects (initialized in __aenter__)
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def __aenter__(self) -> "BaseScraper":
        """
        Async context manager entry - starts browser.

        Sets up Playwright with a Chromium browser and context
        configured for web scraping (appropriate user agent, etc.)
        """
        # Start virtual display if configured (for headed browser on headless machines)
        if self._use_virtual_display:
            VirtualDisplay.acquire()

        self._playwright = await async_playwright().start()

        # Launch browser
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless
        )

        # Create context with realistic browser fingerprint
        self._context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )

        # Set default timeout for all operations
        self._context.set_default_timeout(self.timeout)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Async context manager exit - cleans up browser resources.

        Always closes browser and Playwright, even if an exception occurred.
        """
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        if self._use_virtual_display:
            VirtualDisplay.release()

    async def new_page(self) -> Page:
        """
        Create a new browser page with stealth mode enabled.

        Applies playwright-stealth to avoid bot detection by sites
        using Cloudflare or similar protection.

        Returns:
            New Playwright Page object with stealth enabled
        """
        if not self._context:
            raise RuntimeError("Scraper not initialized. Use 'async with' context manager.")

        page = await self._context.new_page()

        # Apply stealth to avoid Cloudflare and other bot detection
        await _stealth.apply_stealth_async(page)

        return page

    async def navigate(
        self,
        page: Page,
        url: str,
        wait_for: str = "load",
        max_attempts: Optional[int] = None,
    ) -> None:
        """
        Navigate to a URL with retry logic.

        Args:
            page: Playwright Page object
            url: URL to navigate to
            wait_for: Wait condition ('load', 'domcontentloaded', 'networkidle')

        Raises:
            Exception: If navigation fails after all retries
        """
        # Use lambda to create a fresh coroutine on each retry attempt
        await self.with_retry(
            lambda: page.goto(url, wait_until=wait_for, timeout=self.timeout),
            max_attempts=max_attempts,
            description=f"Navigate to {url}",
        )

    async def random_delay(self) -> None:
        """
        Wait for a random duration to avoid rate limiting.

        Delay is between scrape_delay_min and scrape_delay_max
        from settings (default 1-3 seconds).
        """
        delay = random.uniform(settings.scrape_delay_min, settings.scrape_delay_max)
        await asyncio.sleep(delay)

    async def with_retry(
        self,
        coro_func,
        max_attempts: int = None,
        base_delay: float = 2.0,
        description: str = "Operation",
    ):
        """
        Execute an async operation with exponential backoff retry.

        IMPORTANT: Pass a callable (like a lambda) that creates a coroutine,
        not a pre-created coroutine. Coroutines can only be awaited once,
        so we need to create a fresh one for each retry attempt.

        Args:
            coro_func: Callable that returns a coroutine (e.g., lambda: page.goto(url))
            max_attempts: Maximum retry attempts (default from settings)
            base_delay: Initial delay between retries (doubles each attempt)
            description: Description for logging

        Returns:
            Result of the coroutine

        Raises:
            Exception: The last exception if all retries fail

        Example:
            # Correct - lambda creates fresh coroutine each attempt
            await self.with_retry(
                lambda: page.goto(url),
                description="Navigate to page"
            )

            # WRONG - coroutine created once, can't be reused
            # await self.with_retry(page.goto(url))  # Don't do this!
        """
        if max_attempts is None:
            max_attempts = settings.scrape_max_retries

        last_error = None

        for attempt in range(max_attempts):
            try:
                # Call the function to get a fresh coroutine for each attempt
                return await coro_func()
            except Exception as e:
                last_error = e

                if attempt < max_attempts - 1:
                    # Calculate delay with exponential backoff
                    delay = base_delay * (2 ** attempt)
                    # Add some jitter to avoid thundering herd
                    delay += random.uniform(0, 1)

                    print(
                        f"[Retry {attempt + 1}/{max_attempts}] {description} "
                        f"failed: {e}. Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)

        raise last_error

    # =========================================================================
    # Abstract Methods - Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    async def get_tournament_list(self, year: int) -> list[dict]:
        """
        Get list of tournaments for a given year.

        Args:
            year: Year to get tournaments for

        Returns:
            List of tournament dicts with at least:
            - 'id': Tournament identifier
            - 'name': Tournament name
            - 'level': Tournament level
            - 'surface': Playing surface
            - 'start_date': Tournament start date
        """
        pass

    @abstractmethod
    async def scrape_tournament_results(
        self,
        tournament_id: str,
        year: int,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape completed match results for a tournament.

        Args:
            tournament_id: Tournament identifier
            year: Year of the tournament edition

        Yields:
            ScrapedMatch objects for each match found
        """
        pass

    @abstractmethod
    async def scrape_fixtures(
        self,
        tournament_id: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures for a tournament.

        Args:
            tournament_id: Tournament identifier

        Yields:
            ScrapedFixture objects for each upcoming match
        """
        pass

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _normalize_round(self, round_str: str) -> str:
        """
        Normalize round names to standard format.

        Different sources use different names for rounds:
        - ATP: "Finals", "Semi-Finals", "Quarter-Finals"
        - ITF: "Final", "SF", "QF"

        This method normalizes to our standard format.

        Args:
            round_str: Raw round string from source

        Returns:
            Normalized round code: 'F', 'SF', 'QF', 'R16', 'R32', 'R64', 'R128',
                                  'Q1', 'Q2', 'Q3', 'RR'
        """
        # Strip trailing dashes/spaces common in ATP format (e.g., "FINALS -")
        round_lower = round_str.lower().strip().rstrip(" -")

        # Finals
        if round_lower in ("final", "finals", "f") or round_lower.startswith("final"):
            return "F"

        # Semi-finals
        if round_lower in ("semi-final", "semi-finals", "semifinals", "sf") or "semi" in round_lower:
            return "SF"

        # Quarter-finals
        if round_lower in ("quarter-final", "quarter-finals", "quarterfinals", "qf") or "quarter" in round_lower:
            return "QF"

        # Main draw rounds
        if "16" in round_lower or "round of 16" in round_lower:
            return "R16"
        if "32" in round_lower or "round of 32" in round_lower:
            return "R32"
        if "64" in round_lower or "round of 64" in round_lower:
            return "R64"
        if "128" in round_lower or "round of 128" in round_lower:
            return "R128"

        # Round robin
        if "round robin" in round_lower or round_lower == "rr":
            return "RR"

        # Qualifying rounds
        if "q1" in round_lower or "1st round qualifying" in round_lower:
            return "Q1"
        if "q2" in round_lower or "2nd round qualifying" in round_lower:
            return "Q2"
        if "q3" in round_lower or "3rd round qualifying" in round_lower:
            return "Q3"
        if "qual" in round_lower and "final" in round_lower:
            return "Q3"

        # First/second round variations
        if "1st round" in round_lower or "first round" in round_lower:
            return "R64"  # Assumption - may need context
        if "2nd round" in round_lower or "second round" in round_lower:
            return "R32"
        if "3rd round" in round_lower or "third round" in round_lower:
            return "R16"
        if "4th round" in round_lower or "fourth round" in round_lower:
            return "QF"

        # Default - return as-is (will need manual handling)
        return round_str.upper()

    def _normalize_surface(self, surface_str: str) -> str:
        """
        Normalize surface names to standard format.

        Args:
            surface_str: Raw surface string from source

        Returns:
            Normalized surface: 'Hard', 'Clay', 'Grass', 'Carpet'
        """
        surface_lower = surface_str.lower().strip()

        if "hard" in surface_lower:
            return "Hard"
        if "clay" in surface_lower:
            return "Clay"
        if "grass" in surface_lower:
            return "Grass"
        if "carpet" in surface_lower:
            return "Carpet"

        # Default to Hard (most common)
        return "Hard"

    def _normalize_level(self, level_str: str, tour: str = "atp") -> str:
        """
        Normalize tournament level names.

        Args:
            level_str: Raw level string from source
            tour: Tour type ('atp', 'wta', 'itf')

        Returns:
            Normalized level: 'Grand Slam', 'Masters 1000', 'ATP 500',
                             'ATP 250', 'Challenger', 'ITF'
        """
        level_lower = level_str.lower().strip()

        # Grand Slams (same for all tours)
        if "grand slam" in level_lower or "gs" in level_lower:
            return "Grand Slam"

        # Top tier
        if "1000" in level_lower or "masters" in level_lower:
            return "Masters 1000" if tour == "atp" else "WTA 1000"

        # Mid tier
        if "500" in level_lower:
            return "ATP 500" if tour == "atp" else "WTA 500"

        # Lower tier
        if "250" in level_lower:
            return "ATP 250" if tour == "atp" else "WTA 250"

        # Challenger / WTA 125
        if "125" in level_lower:
            return "WTA 125"

        # Challenger
        if "challenger" in level_lower or "ch" in level_lower:
            return "Challenger"

        # ITF
        if "itf" in level_lower or "future" in level_lower:
            return "ITF"

        # Default based on tour
        if tour == "itf":
            return "ITF"
        elif tour == "challenger":
            return "Challenger"
        else:
            return "ATP 250" if tour == "atp" else "WTA 250"
