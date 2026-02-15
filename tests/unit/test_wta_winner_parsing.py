from bs4 import BeautifulSoup

from teelo.scrape.wta import WTAScraper


def _parse_single_table(table_html: str):
    soup = BeautifulSoup(table_html, "lxml")
    table = soup.select_one("table.match-table")
    assert table is not None
    scraper = WTAScraper()
    return scraper._parse_match_table(
        table=table,
        tournament_id="australian-open",
        tournament_number="901",
        year=2026,
        round_code="F",
        match_number=1,
    )


def test_parse_match_table_uses_set_winner_markers_when_table_winner_class_missing():
    match = _parse_single_table(
        """
        <table class="match-table js-match-status">
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/321379/aryna-sabalenka">A. Sabalenka(1)</a>
            </td>
            <td class="match-table__score-cell">4</td>
            <td class="match-table__score-cell is-winner">6</td>
            <td class="match-table__score-cell">4</td>
          </tr>
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/322122/elena-rybakina">E. Rybakina(5)</a>
            </td>
            <td class="match-table__score-cell is-winner">6</td>
            <td class="match-table__score-cell">4</td>
            <td class="match-table__score-cell is-winner">6</td>
          </tr>
        </table>
        """
    )
    assert match is not None
    assert match.player_a_name == "A. Sabalenka"
    assert match.player_b_name == "E. Rybakina"
    assert match.winner_name == "E. Rybakina"
    assert match.score_raw == "4-6 6-4 4-6"


def test_parse_match_table_prefers_explicit_table_winner_class():
    match = _parse_single_table(
        """
        <table class="match-table match-table--winner-a">
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/100001/player-a">Player A</a>
            </td>
            <td class="match-table__score-cell is-winner">6</td>
            <td class="match-table__score-cell is-winner">6</td>
          </tr>
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/100002/player-b">Player B</a>
            </td>
            <td class="match-table__score-cell">4</td>
            <td class="match-table__score-cell">2</td>
          </tr>
        </table>
        """
    )
    assert match is not None
    assert match.winner_name == "Player A"
