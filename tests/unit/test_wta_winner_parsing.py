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


def _parse_single_draw_table(table_html: str):
    soup = BeautifulSoup(table_html, "lxml")
    table = soup.select_one("table.match-table")
    assert table is not None
    scraper = WTAScraper()
    return scraper._parse_draw_entry_table(
        table=table,
        tournament_id="indian-wells",
        year=2026,
        round_code="R64",
        draw_position=1,
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


def test_parse_draw_entry_keeps_missing_opponent_as_incomplete_not_bye():
    entry = _parse_single_draw_table(
        """
        <table class="match-table">
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/100001/player-a">Player A(1)</a>
            </td>
          </tr>
          <tr class="match-table__row">
            <td class="match-table__player-cell"></td>
          </tr>
        </table>
        """
    )
    assert entry is not None
    assert entry.player_a_name == "Player A"
    assert entry.player_b_name is None
    assert entry.is_bye is False


def test_parse_match_table_skips_upcoming_match_not_yet_started():
    # Mirrors the Linz 2026 final card: upcoming match with all score cells
    # showing placeholder dots. Previously this was silently recorded as a
    # walkover with no winner; the finished-gate must reject it.
    match = _parse_single_table(
        """
        <div class="tennis-match js-match-foo" data-status="U">
          <div class="match-table__container js-match-container">
            <table class="match-table js-match-status">
              <tr class="match-table__row js-team-a">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/331809/mirra-andreeva">M. Andreeva</a>
                </td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
              </tr>
              <tr class="match-table__row js-team-b">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/328013/anastasia-potapova">A. Potapova</a>
                </td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
              </tr>
            </table>
          </div>
        </div>
        """
    )
    assert match is None


def test_parse_match_table_skips_live_match_with_one_set_in_progress():
    # Live match: first set won, second set in progress. The `is-winner` cells
    # on the finished set must not be enough to flag the match as completed.
    match = _parse_single_table(
        """
        <div class="tennis-match" data-status="L">
          <div class="match-table__container js-match-container">
            <table class="match-table js-match-status">
              <tr class="match-table__row">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/100001/player-a">Player A</a>
                </td>
                <td class="match-table__score-cell is-winner">6</td>
                <td class="match-table__score-cell">3</td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
              </tr>
              <tr class="match-table__row">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/100002/player-b">Player B</a>
                </td>
                <td class="match-table__score-cell">4</td>
                <td class="match-table__score-cell">2</td>
                <td class="match-table__score-cell"><span class="match-table__dot">.</span></td>
              </tr>
            </table>
          </div>
        </div>
        """
    )
    assert match is None


def test_parse_match_table_accepts_finished_outer_status_without_winner_class():
    # Draws-page finished matches have no `match-table__finished` container
    # class and no `data-completed` attribute — the only reliable finished
    # signal on those is `data-status="F"` on the outer wrapper.
    match = _parse_single_table(
        """
        <div class="tennis-match" data-status="F">
          <table class="match-table match-table--winner-b">
            <tr class="match-table__row">
              <td class="match-table__player-cell">
                <a class="match-table__player--link" href="/players/100001/player-a">Player A</a>
              </td>
              <td class="match-table__score-cell">4</td>
              <td class="match-table__score-cell">2</td>
            </tr>
            <tr class="match-table__row">
              <td class="match-table__player-cell">
                <a class="match-table__player--link" href="/players/100002/player-b">Player B</a>
              </td>
              <td class="match-table__score-cell is-winner">6</td>
              <td class="match-table__score-cell is-winner">6</td>
            </tr>
          </table>
        </div>
        """
    )
    assert match is not None
    assert match.winner_name == "Player B"
    assert match.score_raw == "4-6 2-6"


def test_parse_match_table_accepts_finished_with_intermediate_tennis_match_wrapper():
    # Regression for _is_tennis_match_wrapper bug: ' '.join(c) was joining
    # characters of the class string rather than a list, so 'tennis-match__'
    # was never found in the joined output. This caused tennis-match__container
    # (and similar inner divs) to match the predicate, so find_parent latched
    # onto the wrong ancestor instead of the outer div.tennis-match[data-status="F"].
    match = _parse_single_table(
        """
        <div class="tennis-match" data-status="F">
          <div class="tennis-match__container js-match-container">
            <table class="match-table js-match-status">
              <tr class="match-table__row">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/100001/player-a">Player A</a>
                </td>
                <td class="match-table__score-cell">3</td>
                <td class="match-table__score-cell">4</td>
              </tr>
              <tr class="match-table__row">
                <td class="match-table__player-cell">
                  <a class="match-table__player--link" href="/players/100002/player-b">Player B</a>
                </td>
                <td class="match-table__score-cell is-winner">6</td>
                <td class="match-table__score-cell is-winner">6</td>
              </tr>
            </table>
          </div>
        </div>
        """
    )
    assert match is not None
    assert match.winner_name == "Player B"
    assert match.score_raw == "3-6 4-6"


def test_parse_draw_entry_marks_explicit_bye_as_bye():
    entry = _parse_single_draw_table(
        """
        <table class="match-table">
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <a class="match-table__player--link" href="/players/100001/player-a">Player A(1)</a>
            </td>
          </tr>
          <tr class="match-table__row">
            <td class="match-table__player-cell">
              <div class="match-table__player">Bye</div>
            </td>
          </tr>
        </table>
        """
    )
    assert entry is not None
    assert entry.player_a_name == "Player A"
    assert entry.player_b_name is None
    assert entry.is_bye is True
