#!/usr/bin/env python3
from __future__ import annotations

"""
Wait Strategy Benchmark Script.

A near-complete replica of update_current_events.py that:
1. Discovers currently running tournaments using the same window logic
2. Calls the actual scraper methods (scrape_tournament_results, scrape_tournament_draw)
3. Discards results instead of writing to DB — pure timing measurement
4. Records per-tournament: time taken, match count, draw entry count, errors
5. Writes a JSON log to logs/wait_benchmark_TIMESTAMP.json

Two strategies are implemented:
  baseline  — Uses existing scraper methods unchanged (mirrors production)
  optimized — Modified versions that remove redundant waits:
               ATP:  remove random_delay() after wait_for_selector confirms content
               ITF:  replace wait_for_load_state("networkidle") with
                     wait_for_function that fires when DOM title changes

Usage:
    # Compare both strategies on 5 tournaments per tour
    python3 scripts/test_wait_strategy.py --strategy both --tours ATP,CHALLENGER --max-tournaments 5

    # Run only baseline
    python3 scripts/test_wait_strategy.py --strategy baseline --tours ATP,ITF_MEN

    # Run only optimized
    python3 scripts/test_wait_strategy.py --strategy optimized --tours ITF_MEN --max-tournaments 3
"""

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import AsyncGenerator, Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import re as _re

from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from teelo.config import settings
from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.discovery import discover_tournament_tasks
from teelo.scrape.itf import ITFScraper, _normalize_round, _parse_match_widget, _parse_draw_entry_widget
from teelo.scrape.pipeline import TaskParams
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper


# =============================================================================
# Optimized ATP scraper subclass
# =============================================================================

class ATPScraperOptimized(ATPScraper):
    """
    ATPScraper with redundant waits removed.

    Production ATP code calls random_delay() (0.1–0.4 s in fast mode, 1–3 s
    normal) AFTER wait_for_selector() has already confirmed the critical
    element is present.  The delay was added as a safety buffer, but
    wait_for_selector already provides that guarantee.  Removing it costs
    nothing in reliability and saves 0.1–3 s per page load.

    Changes:
      - scrape_tournament_results: drop random_delay() after .match
      - scrape_tournament_draw:    drop random_delay() after .draw-item
      - get_tournament_list:       drop random_delay() after selector
      - _get_tournament_number:    drop random_delay() (uses same page)
      - seed_tournament_info_cache: new helper — caller pre-populates the info
                                    cache from task_params, so scrape methods
                                    never navigate to the overview page at all
    """

    async def get_tournament_list(self, year: int, tour_type: str = "main") -> list[dict]:
        """Optimized: no random_delay() after selector is confirmed."""
        from teelo.scrape.atp_tournament_parser import parse_tournament_elements

        page = await self.new_page()
        tournaments = []
        try:
            url = f"{self.BASE_URL}/en/scores/results-archive?year={year}"
            if tour_type == "challenger":
                url += "&tournamentType=ch"
            await self.navigate(page, url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(".tournament-list, .results-archive-table", timeout=4000)
            except PlaywrightTimeout:
                pass
            # ← No random_delay() here; selector confirms readiness
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            tournaments = parse_tournament_elements(soup, year)
            if not tournaments:
                tournaments = await self._get_tournaments_from_schedule(page, year, tour_type)
        finally:
            await page.close()
        return tournaments

    async def _get_tournament_number_opt(
        self,
        page: Page,
        tournament_id: str,
        year: int,
        tour_type: str = "main",
    ) -> Optional[str]:
        """Like _get_tournament_number but without random_delay()."""
        cache_key = (tournament_id, year, tour_type)
        if cache_key in self._tournament_number_cache:
            return self._tournament_number_cache[cache_key]

        url = f"{self.BASE_URL}/en/scores/results-archive?year={year}"
        if tour_type == "challenger":
            url += "&tournamentType=ch"
        await self.navigate(page, url, wait_for="domcontentloaded")
        try:
            await page.wait_for_selector(".tournament-list, .results-archive-table", timeout=4000)
        except PlaywrightTimeout:
            pass
        # ← No random_delay()

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        pattern = rf"/en/scores/archive/{_re.escape(tournament_id)}/(\d+)/{year}/results"
        for link in soup.find_all("a", href=True):
            m = _re.search(pattern, link.get("href", ""))
            if m:
                number = m.group(1)
                self._tournament_number_cache[cache_key] = number
                return number
        self._tournament_number_cache[cache_key] = None
        return None

    def seed_tournament_info_cache(self, params: "TaskParams", tour_type: str) -> None:
        """
        Pre-populate _tournament_info_cache from TaskParams so that
        scrape_tournament_draw and scrape_tournament_results never need to
        navigate to the overview page.

        The archive page (already scraped during discovery) provides all the
        metadata we need — level, surface, name, dates — via task_params.
        The overview page navigation in production code exists to enrich this
        data, but for benchmark purposes (and ideally in production) the
        archive data is sufficient.

        Key insight: the overview page is an EXTRA page load (~0.3–0.8 s) that
        mostly just verifies data we already have.  By caching from params we
        skip it entirely.
        """
        cache_key = (params.tournament_id, params.year, tour_type, params.tournament_number)
        if cache_key in self._tournament_info_cache:
            return
        info = {
            "id": params.tournament_id,
            "name": params.tournament_name or params.tournament_id.replace("-", " ").title(),
            "year": params.year,
            "level": params.tournament_level or self._detect_level_from_id(params.tournament_id, tour_type),
            "surface": params.tournament_surface or "Hard",
            "location": params.tournament_location or "",
            "country_ioc": None,
            "start_date": params.start_date,
            "end_date": params.end_date,
        }
        self._tournament_info_cache[cache_key] = info

    async def scrape_tournament_results(
        self,
        tournament_id: str,
        year: int,
        tournament_number: Optional[str] = None,
        tour_type: str = "main",
    ) -> AsyncGenerator:
        """Optimized: no random_delay() after .match element is confirmed."""
        page = await self.new_page()
        try:
            if not tournament_number:
                tournament_number = await self._get_tournament_number_opt(
                    page, tournament_id, year, tour_type
                )
                if not tournament_number:
                    return

            tournament_info = await self._get_tournament_info(
                page, tournament_id, year, tour_type, tournament_number=tournament_number
            )
            tournament_info["number"] = tournament_number

            results_url = (
                f"{self.BASE_URL}/en/scores/archive/"
                f"{tournament_id}/{tournament_number}/{year}/results"
            )
            await self.navigate(page, results_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(".match", timeout=4000)
            except Exception:
                pass
            # ← No random_delay(); .match presence confirms JS render

            html = await page.content()
            async for match in self._parse_results_page(html, tournament_info, "main"):
                yield match
        finally:
            await page.close()

    async def scrape_tournament_draw(
        self,
        tournament_id: str,
        year: int,
        tournament_number: Optional[str] = None,
        tour_type: str = "main",
    ) -> list:
        """Optimized: no random_delay() after .draw-item is confirmed."""
        page = await self.new_page()
        entries = []
        try:
            if not tournament_number:
                tournament_number = await self._get_tournament_number_opt(
                    page, tournament_id, year, tour_type
                )
                if not tournament_number:
                    return entries

            tournament_info = await self._get_tournament_info(
                page, tournament_id, year, tour_type, tournament_number=tournament_number
            )

            draws_url = (
                f"{self.BASE_URL}/en/scores/archive/"
                f"{tournament_id}/{tournament_number}/{year}/draws"
            )
            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(".draw-item", timeout=4000)
            except Exception:
                pass
            # ← No random_delay()

            html = await page.content()
            entries = self._parse_draw_page(html, tournament_info)
        finally:
            await page.close()
        return entries


# =============================================================================
# Optimized ITF scraper subclass
# =============================================================================

class ITFScraperOptimized(ITFScraper):
    """
    ITFScraper with networkidle carousel waits replaced by DOM-change waits.

    Production ITF code calls wait_for_load_state("networkidle", timeout=4000)
    after each carousel next-button click.  "networkidle" waits until ALL
    pending network requests settle, which can take the full 4 s even when
    the new round DOM is already rendered.

    Optimized approach: after clicking next, capture the current set of round
    titles, then wait_for_function until the titles change.  This fires the
    instant the carousel DOM updates — typically <200 ms instead of 1–4 s.
    """

    async def scrape_tournament_results(
        self,
        tournament_url: str,
        tournament_info: dict,
    ) -> AsyncGenerator:
        """Optimized carousel: DOM-change wait instead of networkidle."""
        page = await self.new_page()
        try:
            draws_url = tournament_url.rstrip("/") + "/draws-and-results/"
            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(
                    ".drawsheet-round-container, .drawsheet-widget", timeout=4000
                )
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            seen_rounds: set[str] = set()
            match_number = 0

            for view_idx in range(3):
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                for container in soup.select(".drawsheet-round-container"):
                    title_elem = container.select_one(".drawsheet-round-container__round-title")
                    if not title_elem:
                        continue
                    round_name = _normalize_round(title_elem.get_text(strip=True))
                    if round_name in seen_rounds:
                        continue
                    seen_rounds.add(round_name)

                    for widget in container.select(".drawsheet-widget"):
                        match = _parse_match_widget(widget, round_name, tournament_info, match_number)
                        if match:
                            match_number += 1
                            match.match_number = match_number
                            yield match

                if view_idx < 2:
                    try:
                        next_btn = await page.wait_for_selector("button.btn--chevron-next", timeout=4000)
                        if next_btn and await next_btn.is_visible():
                            # Capture current titles before clicking so we can detect change
                            current_titles = await page.evaluate(
                                "() => Array.from("
                                "document.querySelectorAll('.drawsheet-round-container__round-title')"
                                ").map(el => el.textContent.trim())"
                            )
                            await next_btn.click()
                            # Wait for DOM update rather than network idle.
                            # The first title changing signals carousel advance.
                            try:
                                await page.wait_for_function(
                                    "(prevTitles) => {"
                                    "  const newTitles = Array.from("
                                    "    document.querySelectorAll('.drawsheet-round-container__round-title')"
                                    "  ).map(el => el.textContent.trim());"
                                    "  return newTitles.some((t, i) => t !== prevTitles[i]);"
                                    "}",
                                    arg=current_titles,
                                    timeout=4000,
                                )
                                # 100ms buffer: the first title changed but the new
                                # third column may still be rendering its widgets.
                                await asyncio.sleep(0.1)
                            except PlaywrightTimeout:
                                await asyncio.sleep(0.3)
                        else:
                            break
                    except PlaywrightTimeout:
                        break
        finally:
            await page.close()

    async def scrape_tournament_draw(
        self,
        tournament_url: str,
        tournament_info: dict,
    ) -> list:
        """Optimized carousel draw: DOM-change wait instead of networkidle."""
        page = await self.new_page()
        entries = []
        try:
            draws_url = tournament_url.rstrip("/") + "/draws-and-results/"
            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(
                    ".drawsheet-round-container, .drawsheet-widget", timeout=4000
                )
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            seen_rounds: set[str] = set()

            for view_idx in range(3):
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                for container in soup.select(".drawsheet-round-container"):
                    title_elem = container.select_one(".drawsheet-round-container__round-title")
                    if not title_elem:
                        continue
                    round_name = _normalize_round(title_elem.get_text(strip=True))
                    if round_name in seen_rounds:
                        continue
                    seen_rounds.add(round_name)

                    widgets = container.select(".drawsheet-widget")
                    for i, widget in enumerate(widgets):
                        entry = _parse_draw_entry_widget(widget, round_name, i + 1, tournament_info)
                        if entry:
                            entries.append(entry)

                if view_idx < 2:
                    try:
                        next_btn = await page.wait_for_selector("button.btn--chevron-next", timeout=4000)
                        if next_btn and await next_btn.is_visible():
                            current_titles = await page.evaluate(
                                "() => Array.from("
                                "document.querySelectorAll('.drawsheet-round-container__round-title')"
                                ").map(el => el.textContent.trim())"
                            )
                            await next_btn.click()
                            try:
                                await page.wait_for_function(
                                    "(prevTitles) => {"
                                    "  const newTitles = Array.from("
                                    "    document.querySelectorAll('.drawsheet-round-container__round-title')"
                                    "  ).map(el => el.textContent.trim());"
                                    "  return newTitles.some((t, i) => t !== prevTitles[i]);"
                                    "}",
                                    arg=current_titles,
                                    timeout=4000,
                                )
                                # 100ms buffer: title changed but new column may still be rendering
                                await asyncio.sleep(0.1)
                            except PlaywrightTimeout:
                                await asyncio.sleep(0.3)
                        else:
                            break
                    except PlaywrightTimeout:
                        break
        finally:
            await page.close()
        return entries


# =============================================================================
# Benchmark helpers
# =============================================================================

def _get_scraper_class(tour_key: str, optimized: bool):
    """Return the appropriate scraper class for a given tour and strategy."""
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if optimized:
        if scraper_type == "atp":
            return ATPScraperOptimized
        if scraper_type == "itf":
            return ITFScraperOptimized
        return WTAScraper  # No WTA optimizations in scope
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "itf":
        return ITFScraper
    return WTAScraper


def _itf_tournament_info(params: TaskParams) -> dict:
    """Build the ITF tournament_info dict from a TaskParams."""
    return {
        "id": params.tournament_id,
        "name": params.tournament_name or params.tournament_id,
        "year": params.year,
        "level": params.tournament_level or "ITF",
        "surface": params.tournament_surface or "Hard",
        "location": params.tournament_location,
        "gender": params.gender or "men",
    }


async def _discover_tasks_for_tour(
    tour_key: str,
    year: int,
    headless: bool,
    lookback_days: int,
) -> list[TaskParams]:
    """Discover current tournaments for one tour; returns list of TaskParams."""
    today = date.today()
    window = (today - timedelta(days=lookback_days), today + timedelta(days=7))
    scraper_cls = _get_scraper_class(tour_key, optimized=False)
    async with scraper_cls(headless=headless) as scraper:
        tasks = await discover_tournament_tasks(
            tour_key, year, task_type="current_tournament",
            scraper=scraper, window=window,
        )
    return [t.params for t in tasks]


async def _scrape_one(
    params: TaskParams,
    scraper,
    tour_key: str,
    strategy: str,
) -> dict:
    """
    Run draw + results scrape for a single tournament; discard output.

    Returns a timing/count dict — no DB writes.
    """
    tour_config = TOUR_TYPES[tour_key]
    result: dict = {
        "tournament_id": params.tournament_id,
        "tournament_name": params.tournament_name,
        "tour_key": tour_key,
        "strategy": strategy,
        "matches_found": 0,
        "draw_entries_found": 0,
        "errors": [],
        "phases": {},
        "total_s": 0.0,
    }
    total_start = perf_counter()

    # For optimized ATP scraper: pre-populate the tournament info cache from
    # task_params so that scrape_tournament_draw / scrape_tournament_results
    # never need to navigate to the overview page.  The archive page (already
    # visited during discovery) already gave us all the data we need.
    if isinstance(scraper, ATPScraperOptimized) and not tour_key.startswith("ITF"):
        scraper.seed_tournament_info_cache(params, tour_config.get("tour_type", "main"))

    # ── Draw ──────────────────────────────────────────────────────────────────
    draw_start = perf_counter()
    try:
        if tour_key.startswith("ITF"):
            draw_kwargs: dict = {
                "tournament_url": params.tournament_url,
                "tournament_info": _itf_tournament_info(params),
            }
        else:
            draw_kwargs = {"tournament_id": params.tournament_id, "year": params.year}
            if tour_key in ("ATP", "CHALLENGER"):
                draw_kwargs["tournament_number"] = params.tournament_number
                draw_kwargs["tour_type"] = tour_config["tour_type"]
            elif tour_key in ("WTA", "WTA_125"):
                draw_kwargs["tournament_number"] = params.tournament_number

        entries = await scraper.scrape_tournament_draw(**draw_kwargs)
        result["draw_entries_found"] = len(entries)
    except Exception as exc:
        result["errors"].append(f"draw: {exc}")
    result["phases"]["draw_s"] = perf_counter() - draw_start

    # ── Results ───────────────────────────────────────────────────────────────
    results_start = perf_counter()
    try:
        if tour_key.startswith("ITF"):
            res_kwargs: dict = {
                "tournament_url": params.tournament_url,
                "tournament_info": _itf_tournament_info(params),
            }
        else:
            res_kwargs = {"tournament_id": params.tournament_id, "year": params.year}
            if tour_key in ("ATP", "CHALLENGER"):
                res_kwargs["tournament_number"] = params.tournament_number
                res_kwargs["tour_type"] = tour_config["tour_type"]
            elif tour_key in ("WTA", "WTA_125"):
                res_kwargs["tournament_number"] = params.tournament_number

        matches = []
        async for match in scraper.scrape_tournament_results(**res_kwargs):
            matches.append(match)
        result["matches_found"] = len(matches)
    except Exception as exc:
        result["errors"].append(f"results: {exc}")
    result["phases"]["results_s"] = perf_counter() - results_start

    result["total_s"] = perf_counter() - total_start
    return result


async def run_strategy(
    strategy: str,
    tasks_by_tour: dict[str, list[TaskParams]],
    headless: bool,
    max_tournaments: Optional[int],
) -> list[dict]:
    """
    Run one strategy across all tours.

    Uses one scraper instance per tour (same as production) so browser startup
    isn't counted per-tournament.
    """
    optimized = (strategy == "optimized")
    all_results: list[dict] = []

    for tour_key, task_list in tasks_by_tour.items():
        if max_tournaments:
            task_list = task_list[:max_tournaments]
        if not task_list:
            continue

        print(f"\n[{strategy.upper()} | {tour_key}] {len(task_list)} tournament(s)")
        scraper_cls = _get_scraper_class(tour_key, optimized)

        async with scraper_cls(headless=headless) as scraper:
            for i, params in enumerate(task_list, 1):
                label = params.tournament_name or params.tournament_id
                print(f"  [{i}/{len(task_list)}] {label} ...", end=" ", flush=True)
                r = await _scrape_one(params, scraper, tour_key, strategy)
                print(
                    f"{r['total_s']:.2f}s "
                    f"(draw={r['phases']['draw_s']:.2f}s "
                    f"results={r['phases']['results_s']:.2f}s) "
                    f"matches={r['matches_found']} "
                    f"entries={r['draw_entries_found']}"
                    + (f" ERRORS={r['errors']}" if r["errors"] else "")
                )
                all_results.append(r)

    return all_results


def print_comparison_table(results: list[dict]) -> None:
    """Print a side-by-side comparison table and aggregate summary."""
    baseline_map = {r["tournament_id"]: r for r in results if r["strategy"] == "baseline"}
    optimized_list = [r for r in results if r["strategy"] == "optimized"]

    print("\n" + "=" * 85)
    print("COMPARISON TABLE")
    print("=" * 85)
    print(f"{'Tournament':<38} {'Tour':<12} {'Base (s)':<10} {'Opt (s)':<10} {'Speedup':<8} {'Match Δ'}")
    print("-" * 85)

    for opt in optimized_list:
        tid = opt["tournament_id"]
        base = baseline_map.get(tid)
        name = (opt.get("tournament_name") or tid)[:36]
        tour = opt["tour_key"]
        if base:
            base_t, opt_t = base["total_s"], opt["total_s"]
            speedup = base_t / opt_t if opt_t > 0 else float("inf")
            match_delta = opt["matches_found"] - base["matches_found"]
            delta_str = f"{match_delta:+d}"
            print(f"{name:<38} {tour:<12} {base_t:<10.2f} {opt_t:<10.2f} {speedup:<8.2f} {delta_str}")
        else:
            print(f"{name:<38} {tour:<12} {'n/a':<10} {opt['total_s']:<10.2f} {'n/a':<8} n/a")

    if baseline_map and optimized_list:
        # Only sum tournaments that appear in both strategies for a fair comparison
        common_ids = set(baseline_map) & {r["tournament_id"] for r in optimized_list}
        total_base = sum(baseline_map[tid]["total_s"] for tid in common_ids)
        total_opt = sum(r["total_s"] for r in optimized_list if r["tournament_id"] in common_ids)
        speedup = total_base / total_opt if total_opt > 0 else float("inf")
        total_matches_base = sum(baseline_map[tid]["matches_found"] for tid in common_ids)
        total_matches_opt = sum(r["matches_found"] for r in optimized_list if r["tournament_id"] in common_ids)

        print("-" * 85)
        print(
            f"{'TOTAL':<38} {'':<12} {total_base:<10.2f} {total_opt:<10.2f} "
            f"{speedup:<8.2f} {total_matches_opt - total_matches_base:+d}"
        )
        saved_pct = (1 - total_opt / total_base) * 100 if total_base > 0 else 0
        print(f"\nTotal saved: {total_base - total_opt:.2f}s ({saved_pct:.1f}% faster)")


# =============================================================================
# Entry point
# =============================================================================

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark baseline vs. optimized scraper wait strategies"
    )
    parser.add_argument(
        "--strategy",
        choices=["baseline", "optimized", "both"],
        default="both",
        help="Which strategy to run (default: both)",
    )
    parser.add_argument(
        "--tours",
        default="ATP,CHALLENGER,ITF_MEN,ITF_WOMEN",
        help="Comma-separated tour keys (default: all supported)",
    )
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument(
        "--lookback-days", type=int, default=7,
        help="Discovery window lookback in days (default: 7)",
    )
    parser.add_argument(
        "--max-tournaments", type=int, default=0,
        help="Max tournaments per tour per strategy (0 = all, default: 0)",
    )
    parser.add_argument(
        "--headed", action="store_true",
        help="Force headed browser (needed for ATP Cloudflare bypass)",
    )
    parser.add_argument("--fast", dest="fast", action="store_true", default=True)
    parser.add_argument("--no-fast", dest="fast", action="store_false")
    args = parser.parse_args()

    # Apply fast mode scrape settings (mirrors update_current_events)
    if args.fast:
        settings.scrape_delay_min = 0.1
        settings.scrape_delay_max = 0.4
        settings.scrape_timeout = min(settings.scrape_timeout, 10000)
        settings.scrape_max_retries = min(settings.scrape_max_retries, 2)

    tours = [t.strip().upper() for t in args.tours.split(",")]
    tours = [t for t in tours if t in TOUR_TYPES]
    headless = not args.headed
    max_t = args.max_tournaments if args.max_tournaments > 0 else None

    # Start virtual display if needed (headed browser on headless machines)
    if settings.scrape_virtual_display and not headless:
        print("Starting virtual display...")
        VirtualDisplay.ensure_running()

    print("=" * 60)
    print("WAIT STRATEGY BENCHMARK")
    print(f"Strategy : {args.strategy}")
    print(f"Tours    : {tours}")
    print(f"Year     : {args.year}")
    print(f"Lookback : {args.lookback_days} days")
    print(f"Max/tour : {max_t or 'all'}")
    print(f"Headless : {headless}")
    print(f"Fast mode: {args.fast}  delays={settings.scrape_delay_min}–{settings.scrape_delay_max}s")
    print("=" * 60)

    # ── Discovery (shared across strategies for fairness) ────────────────────
    print("\n[DISCOVERY] Finding current tournaments...")
    tasks_by_tour: dict[str, list[TaskParams]] = {}
    for tour_key in tours:
        print(f"  {tour_key} ...", end=" ", flush=True)
        t0 = perf_counter()
        try:
            params_list = await _discover_tasks_for_tour(
                tour_key, args.year, headless=headless, lookback_days=args.lookback_days
            )
            print(f"{len(params_list)} found in {perf_counter()-t0:.1f}s")
        except Exception as exc:
            print(f"FAILED: {exc}")
            params_list = []
        tasks_by_tour[tour_key] = params_list

    total_discovered = sum(len(v) for v in tasks_by_tour.values())
    print(f"\nTotal discovered: {total_discovered}")
    if total_discovered == 0:
        print("No tournaments found. Try --lookback-days 14 or check --year.")
        return

    # ── Run strategies ────────────────────────────────────────────────────────
    strategies = ["baseline", "optimized"] if args.strategy == "both" else [args.strategy]
    all_results: list[dict] = []

    for strategy in strategies:
        print(f"\n{'='*60}\nRunning: {strategy.upper()}\n{'='*60}")
        t_strat = perf_counter()
        results = await run_strategy(strategy, tasks_by_tour, headless, max_t)
        elapsed = perf_counter() - t_strat
        total_matches = sum(r["matches_found"] for r in results)
        total_entries = sum(r["draw_entries_found"] for r in results)
        print(f"\n[{strategy.upper()}] Done in {elapsed:.2f}s | matches={total_matches} entries={total_entries}")
        all_results.extend(results)

    # ── Comparison table ──────────────────────────────────────────────────────
    if args.strategy == "both":
        print_comparison_table(all_results)

    # ── JSON log ──────────────────────────────────────────────────────────────
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    log_path = Path(__file__).parent.parent / "logs" / f"wait_benchmark_{timestamp}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_payload = {
        "benchmark_ts": timestamp,
        "args": {
            "strategy": args.strategy,
            "tours": tours,
            "year": args.year,
            "lookback_days": args.lookback_days,
            "max_tournaments": max_t,
            "fast_mode": args.fast,
        },
        "settings": {
            "delay_min_s": settings.scrape_delay_min,
            "delay_max_s": settings.scrape_delay_max,
            "timeout_ms": settings.scrape_timeout,
        },
        "results": all_results,
    }
    log_path.write_text(json.dumps(log_payload, indent=2))
    print(f"\nLog written to: {log_path}")


if __name__ == "__main__":
    asyncio.run(main())
