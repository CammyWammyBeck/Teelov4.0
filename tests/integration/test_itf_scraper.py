
import asyncio
import pytest
from teelo.scrape.itf import ITFScraper

@pytest.mark.asyncio
async def test_itf_monastir_perfection():
    url = "https://www.itftennis.com/en/tournament/m15-monastir/tun/2026/m-itf-tun-2026-009/"
    tournament_info = {
        "id": "m-itf-tun-2026-009",
        "name": "M15 Monastir",
        "year": 2026,
        "level": "ITF $15K",
        "surface": "Hard"
    }
    
    matches = []
    async with ITFScraper() as scraper:
        async for match in scraper.scrape_tournament_results(url, tournament_info):
            matches.append(match)
            
    print(f"Total matches: {len(matches)}")
    
    # Sort by round priority
    round_order = {"F": 0, "SF": 1, "QF": 2, "R16": 3, "R32": 4}
    matches.sort(key=lambda m: round_order.get(m.round, 99))
    
    for m in matches:
        print(f"  {m.round:3}: {m.player_a_name} vs {m.player_b_name} -> {m.winner_name} ({m.score_raw})")
    
    # Assertions
    assert len(matches) == 31, f"Expected 31 matches, got {len(matches)}"
    
    rounds = {}
    for m in matches:
        rounds[m.round] = rounds.get(m.round, 0) + 1
        
    # Standard 32-player draw rounds
    assert rounds.get("R32") == 16, f"Expected 16 matches in R32, got {rounds.get('R32')}"
    assert rounds.get("R16") == 8, f"Expected 8 matches in R16, got {rounds.get('R16')}"
    assert rounds.get("QF") == 4, f"Expected 4 matches in QF, got {rounds.get('QF')}"
    assert rounds.get("SF") == 2, f"Expected 2 matches in SF, got {rounds.get('SF')}"
    assert rounds.get("F") == 1, f"Expected 1 match in F, got {rounds.get('F')}"
    
    # Check a few specific matches for data perfection
    # For example, the final
    final = next(m for m in matches if m.round == "F")
    assert final.player_a_name and final.player_b_name
    assert final.winner_name in [final.player_a_name, final.player_b_name]
    assert "-" in final.score_raw
    
    print("Test passed!")

async def test_itf_marrakech_perfection():
    url = "https://www.itftennis.com/en/tournament/m25-marrakech/mar/2026/m-itf-mar-2026-001/"
    tournament_info = {
        "id": "m-itf-mar-2026-001",
        "name": "M25 Marrakech",
        "year": 2026,
        "level": "ITF $25K",
        "surface": "Clay"
    }
    
    matches = []
    async with ITFScraper() as scraper:
        async for match in scraper.scrape_tournament_results(url, tournament_info):
            matches.append(match)
            
    print(f"Total matches Marrakech: {len(matches)}")
    assert len(matches) == 31, f"Expected 31 matches, got {len(matches)}"
    
    # Sort by round priority
    round_order = {"F": 0, "SF": 1, "QF": 2, "R16": 3, "R32": 4}
    matches.sort(key=lambda m: round_order.get(m.round, 99))
    
    for m in matches:
        print(f"  {m.round:3}: {m.player_a_name} vs {m.player_b_name} -> {m.winner_name} ({m.score_raw})")

if __name__ == "__main__":
    async def run_tests():
        await test_itf_monastir_perfection()
        await test_itf_marrakech_perfection()
        
    asyncio.run(run_tests())
