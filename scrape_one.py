"""Scrape a single HLTV match and print the results to stdout.

Usage:
    cd hltv-scraper
    python3 scrape_one.py              # scrape + print
    python3 scrape_one.py --print-only # print stored data, no scraping

Make sure the database is running first:
    docker compose up -d postgres
"""

import argparse
import asyncio
import sys
import time

# ---------------------------------------------------------------------------
# Match to scrape
# ---------------------------------------------------------------------------
MATCH_ID = 2393046
MATCH_URL = "/matches/2393046/astralis-vs-fut-pgl-bucharest-2026"


def print_results(match_repo) -> None:
    """Read data for MATCH_ID from the DB and print it."""
    match = match_repo.get_match(MATCH_ID)
    maps  = match_repo.get_maps(MATCH_ID)
    vetos = match_repo.get_vetoes(MATCH_ID)

    print("=" * 60)

    # --- Match overview ---
    print("\n### MATCH OVERVIEW")
    if match:
        print(f"  Match ID   : {match['match_id']}")
        print(f"  Date       : {match['date']}")
        print(f"  Event      : {match['event_name']} (id={match['event_id']})")
        print(f"  Teams      : {match['team1_name']} vs {match['team2_name']}")
        print(f"  Score      : {match['team1_score']} – {match['team2_score']}")
        print(f"  Best of    : {match['best_of']}")
        print(f"  LAN        : {match['is_lan']}")
    else:
        print("  (not found in DB)")

    # --- Vetoes ---
    if vetos:
        print("\n### MAP VETOES")
        for v in vetos:
            print(f"  Step {v['step_number']:2d}  {str(v['team_name'] or ''):20s}  {str(v['action'] or ''):6s}  {v['map_name'] or ''}")

    # --- Maps + player stats ---
    print("\n### MAPS")
    for m in maps:
        print(
            f"  Map {m['map_number']}: {m['map_name']}  "
            f"{m['team1_rounds']}–{m['team2_rounds']}  "
            f"(mapstatsid={m['mapstatsid']})"
        )

        stats = match_repo.get_player_stats(MATCH_ID, m["map_number"])
        if stats:
            print(f"    {'Player':<20} {'Team':>6} {'K':>4} {'D':>4} {'A':>4} {'ADR':>6} {'KAST':>6} {'Rating':>7}")
            print(f"    {'-'*20} {'-'*6} {'-'*4} {'-'*4} {'-'*4} {'-'*6} {'-'*6} {'-'*7}")
            for ps in stats:
                kast_str = f"{ps['kast']:.1f}%" if ps['kast'] is not None else "   n/a"
                adr_str  = f"{ps['adr']:.1f}"  if ps['adr']  is not None else "   n/a"
                rat_str  = f"{ps['rating']:.3f}" if ps['rating'] is not None else "    n/a"
                print(
                    f"    {ps['player_name']:<20} {ps['team_id']:>6} "
                    f"{ps['kills']:>4} {ps['deaths']:>4} {ps['assists']:>4} "
                    f"{adr_str:>6} {kast_str:>6} {rat_str:>7}"
                )
        else:
            print("    (no player stats)")

    print("\n" + "=" * 60)


async def main(print_only: bool) -> None:
    from scraper.config import ScraperConfig
    from scraper.db import Database
    from scraper.discovery_repository import DiscoveryRepository
    from scraper.http_client import HLTVClient
    from scraper.pipeline_v2 import _scrape_match
    from scraper.repository import MatchRepository
    from scraper.storage import HtmlStorage

    config = ScraperConfig(save_html=True)

    print("Connecting to database...")
    db = Database()
    db.initialize()
    match_repo = MatchRepository(db.conn)

    if print_only:
        print_results(match_repo)
        db.close()
        return

    discovery_repo = DiscoveryRepository(db.conn)
    storage = HtmlStorage(config.data_dir)

    from datetime import datetime, timezone
    discovery_repo.upsert_batch([{
        "match_id": MATCH_ID,
        "url": MATCH_URL,
        "offset": 0,
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "is_forfeit": 0,
    }])
    print(f"Queued match {MATCH_ID}")

    print("Starting browser (this may take a few seconds)...")
    client = HLTVClient(config)
    await client.start()
    print("Browser ready.\n")

    t0 = time.monotonic()
    print(f"Scraping: {config.base_url}{MATCH_URL}")
    print("-" * 60)

    result = await asyncio.wait_for(
        _scrape_match(
            match_id=MATCH_ID,
            url=MATCH_URL,
            client=client,
            match_repo=match_repo,
            discovery_repo=discovery_repo,
            storage=storage,
            config=config,
        ),
        timeout=config.per_match_timeout,
    )

    elapsed = time.monotonic() - t0
    await client.close()

    if not result["ok"]:
        print(f"\nSCRAPE FAILED: {result['error']}")
        db.close()
        sys.exit(1)

    print(f"\nScrape succeeded in {elapsed:.1f}s  ({result['maps_done']} maps)")
    print_results(match_repo)
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print stored DB data without scraping",
    )
    args = parser.parse_args()
    asyncio.run(main(args.print_only))
