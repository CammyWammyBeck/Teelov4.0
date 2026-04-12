import asyncio

from bs4 import BeautifulSoup

from teelo.db.models import Tournament
from teelo.scrape.atp import ATPScraper
from teelo.scrape.atp_tournament_parser import parse_tournament_elements
from teelo.scrape.pipeline import TaskParams, get_or_create_edition


def test_parse_atp_archive_tournament_surface_left_unknown() -> None:
    html = """
    <div class="tournament-list">
      <ul class="events">
        <li>
          <a class="results" href="/en/scores/archive/barcelona/425/2026/results">Results</a>
          <a class="tournament__profile" href="/en/tournaments/barcelona/425/overview">Profile</a>
          <span class="name">Barcelona</span>
          <span class="venue">Barcelona, Spain | </span>
          <span class="Date">13 April, 2026 - 19 April, 2026</span>
          <img class="events_banner" src="/assets/categorystamps_500.png" />
        </li>
      </ul>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")

    tournaments = parse_tournament_elements(soup, 2026)

    assert len(tournaments) == 1
    assert tournaments[0]["id"] == "barcelona"
    assert tournaments[0]["surface"] is None


def test_get_or_create_edition_prefers_existing_tournament_surface_when_task_surface_missing(db_session) -> None:
    tournament = Tournament(
        tournament_code="barcelona",
        name="Barcelona",
        tour="ATP",
        gender="men",
        level="ATP 500",
        surface="Clay",
    )
    db_session.add(tournament)
    db_session.flush()

    params = TaskParams(
        tournament_id="barcelona",
        year=2026,
        tour_key="ATP",
        tournament_name="Barcelona",
        tournament_level="ATP 500",
        tournament_surface=None,
        tournament_location="Barcelona, Spain",
        tournament_number="425",
        tour_type="main",
    )

    edition = asyncio.run(get_or_create_edition(db_session, params, "ATP"))

    assert edition.surface == "Clay"
    assert edition.tournament_id == tournament.id


def test_get_or_create_edition_leaves_unknown_surface_null_for_new_tournament(db_session) -> None:
    params = TaskParams(
        tournament_id="new-event",
        year=2026,
        tour_key="ATP",
        tournament_name="New Event",
        tournament_level="ATP 250",
        tournament_surface=None,
        tournament_location="Somewhere, Country",
        tournament_number="999",
        tour_type="main",
    )

    edition = asyncio.run(get_or_create_edition(db_session, params, "ATP"))
    tournament = edition.tournament

    assert tournament.surface is None
    assert edition.surface is None


def test_get_tournament_info_fetches_when_seeded_cache_has_no_surface(monkeypatch) -> None:
    scraper = ATPScraper(headless=False)
    params = TaskParams(
        tournament_id="barcelona",
        year=2026,
        tour_key="ATP",
        tournament_name="Barcelona",
        tournament_level="ATP 500",
        tournament_surface=None,
        tournament_location="Barcelona, Spain",
        tournament_number="425",
        tour_type="main",
    )
    scraper.seed_tournament_info_cache(params, tour_type="main")

    class FakePage:
        async def content(self):
            return """
            <html>
              <body>
                <h1>Barcelona</h1>
                <ul class="td_left">
                  <li><span>Surface</span><span>Clay</span></li>
                  <li><span>Location</span><span>Barcelona, Spain</span></li>
                </ul>
              </body>
            </html>
            """

    async def fake_navigate(page, url, wait_for=None):
        return None

    async def fake_random_delay():
        return None

    monkeypatch.setattr(scraper, "navigate", fake_navigate)
    monkeypatch.setattr(scraper, "random_delay", fake_random_delay)

    info = asyncio.run(scraper._get_tournament_info(FakePage(), "barcelona", 2026, "main", "425"))

    assert info["surface"] == "Clay"
    assert info["location"] == "Barcelona, Spain"
