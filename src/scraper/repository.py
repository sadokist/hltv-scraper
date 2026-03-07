"""Data access layer with UPSERT operations for all database tables.

Provides MatchRepository with insert-or-update semantics for matches,
maps, player_stats, round_history, and economy tables.  Each UPSERT
uses INSERT ... ON CONFLICT DO UPDATE SET to modify rows in place.

Batch methods wrap multiple upserts in a single atomic transaction.
Read methods return dicts for easy consumption.
"""

# ---------------------------------------------------------------------------
# UPSERT SQL constants (PostgreSQL syntax)
# ---------------------------------------------------------------------------

UPSERT_MATCH = """
    INSERT INTO matches (
        match_id, date, date_unix_ms, event_id, event_name,
        team1_id, team1_name, team2_id, team2_name,
        team1_score, team2_score, best_of, is_lan,
        match_url, scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(date)s, %(date_unix_ms)s, %(event_id)s, %(event_name)s,
        %(team1_id)s, %(team1_name)s, %(team2_id)s, %(team2_name)s,
        %(team1_score)s, %(team2_score)s, %(best_of)s, %(is_lan)s,
        %(match_url)s, %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id) DO UPDATE SET
        date           = EXCLUDED.date,
        date_unix_ms   = EXCLUDED.date_unix_ms,
        event_id       = EXCLUDED.event_id,
        event_name     = EXCLUDED.event_name,
        team1_id       = EXCLUDED.team1_id,
        team1_name     = EXCLUDED.team1_name,
        team2_id       = EXCLUDED.team2_id,
        team2_name     = EXCLUDED.team2_name,
        team1_score    = EXCLUDED.team1_score,
        team2_score    = EXCLUDED.team2_score,
        best_of        = EXCLUDED.best_of,
        is_lan         = EXCLUDED.is_lan,
        match_url      = EXCLUDED.match_url,
        updated_at     = EXCLUDED.scraped_at,
        source_url     = EXCLUDED.source_url,
        parser_version = EXCLUDED.parser_version
"""

UPSERT_MAP = """
    INSERT INTO maps (
        match_id, map_number, mapstatsid, map_name,
        team1_rounds, team2_rounds,
        team1_ct_rounds, team1_t_rounds,
        team2_ct_rounds, team2_t_rounds,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(map_number)s, %(mapstatsid)s, %(map_name)s,
        %(team1_rounds)s, %(team2_rounds)s,
        %(team1_ct_rounds)s, %(team1_t_rounds)s,
        %(team2_ct_rounds)s, %(team2_t_rounds)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, map_number) DO UPDATE SET
        mapstatsid      = EXCLUDED.mapstatsid,
        map_name        = EXCLUDED.map_name,
        team1_rounds    = EXCLUDED.team1_rounds,
        team2_rounds    = EXCLUDED.team2_rounds,
        team1_ct_rounds = EXCLUDED.team1_ct_rounds,
        team1_t_rounds  = EXCLUDED.team1_t_rounds,
        team2_ct_rounds = EXCLUDED.team2_ct_rounds,
        team2_t_rounds  = EXCLUDED.team2_t_rounds,
        updated_at      = EXCLUDED.scraped_at,
        source_url      = EXCLUDED.source_url,
        parser_version  = EXCLUDED.parser_version
"""

UPSERT_PLAYER_STATS = """
    INSERT INTO player_stats (
        match_id, map_number, player_id, player_name, team_id,
        kills, deaths, assists, flash_assists, hs_kills, kd_diff,
        adr, kast, fk_diff, rating,
        kpr, dpr,
        opening_kills, opening_deaths, multi_kills, clutch_wins,
        traded_deaths, round_swing, mk_rating,
        e_kills, e_deaths, e_hs_kills, e_kd_diff, e_adr, e_kast,
        e_opening_kills, e_opening_deaths, e_fk_diff, e_traded_deaths,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(map_number)s, %(player_id)s, %(player_name)s, %(team_id)s,
        %(kills)s, %(deaths)s, %(assists)s, %(flash_assists)s, %(hs_kills)s, %(kd_diff)s,
        %(adr)s, %(kast)s, %(fk_diff)s, %(rating)s,
        %(kpr)s, %(dpr)s,
        %(opening_kills)s, %(opening_deaths)s, %(multi_kills)s, %(clutch_wins)s,
        %(traded_deaths)s, %(round_swing)s, %(mk_rating)s,
        %(e_kills)s, %(e_deaths)s, %(e_hs_kills)s, %(e_kd_diff)s, %(e_adr)s, %(e_kast)s,
        %(e_opening_kills)s, %(e_opening_deaths)s, %(e_fk_diff)s, %(e_traded_deaths)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, map_number, player_id) DO UPDATE SET
        player_name    = EXCLUDED.player_name,
        team_id        = EXCLUDED.team_id,
        kills          = EXCLUDED.kills,
        deaths         = EXCLUDED.deaths,
        assists        = EXCLUDED.assists,
        flash_assists  = EXCLUDED.flash_assists,
        hs_kills       = EXCLUDED.hs_kills,
        kd_diff        = EXCLUDED.kd_diff,
        adr            = EXCLUDED.adr,
        kast           = EXCLUDED.kast,
        fk_diff        = EXCLUDED.fk_diff,
        rating         = EXCLUDED.rating,
        kpr            = EXCLUDED.kpr,
        dpr            = EXCLUDED.dpr,
        opening_kills  = EXCLUDED.opening_kills,
        opening_deaths = EXCLUDED.opening_deaths,
        multi_kills    = EXCLUDED.multi_kills,
        clutch_wins    = EXCLUDED.clutch_wins,
        traded_deaths  = EXCLUDED.traded_deaths,
        round_swing    = EXCLUDED.round_swing,
        mk_rating      = EXCLUDED.mk_rating,
        e_kills        = EXCLUDED.e_kills,
        e_deaths       = EXCLUDED.e_deaths,
        e_hs_kills     = EXCLUDED.e_hs_kills,
        e_kd_diff      = EXCLUDED.e_kd_diff,
        e_adr          = EXCLUDED.e_adr,
        e_kast         = EXCLUDED.e_kast,
        e_opening_kills  = EXCLUDED.e_opening_kills,
        e_opening_deaths = EXCLUDED.e_opening_deaths,
        e_fk_diff      = EXCLUDED.e_fk_diff,
        e_traded_deaths  = EXCLUDED.e_traded_deaths,
        updated_at     = EXCLUDED.scraped_at,
        source_url     = EXCLUDED.source_url,
        parser_version = EXCLUDED.parser_version
"""

UPSERT_ROUND = """
    INSERT INTO round_history (
        match_id, map_number, round_number,
        winner_side, win_type, winner_team_id,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(map_number)s, %(round_number)s,
        %(winner_side)s, %(win_type)s, %(winner_team_id)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, map_number, round_number) DO UPDATE SET
        winner_side    = EXCLUDED.winner_side,
        win_type       = EXCLUDED.win_type,
        winner_team_id = EXCLUDED.winner_team_id,
        updated_at     = EXCLUDED.scraped_at,
        source_url     = EXCLUDED.source_url,
        parser_version = EXCLUDED.parser_version
"""

UPSERT_ECONOMY = """
    INSERT INTO economy (
        match_id, map_number, round_number, team_id,
        equipment_value, buy_type,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(map_number)s, %(round_number)s, %(team_id)s,
        %(equipment_value)s, %(buy_type)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, map_number, round_number, team_id) DO UPDATE SET
        equipment_value = EXCLUDED.equipment_value,
        buy_type        = EXCLUDED.buy_type,
        updated_at      = EXCLUDED.scraped_at,
        source_url      = EXCLUDED.source_url,
        parser_version  = EXCLUDED.parser_version
"""

UPSERT_VETO = """
    INSERT INTO vetoes (
        match_id, step_number, team_name, action, map_name,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(step_number)s, %(team_name)s, %(action)s, %(map_name)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, step_number) DO UPDATE SET
        team_name      = EXCLUDED.team_name,
        action         = EXCLUDED.action,
        map_name       = EXCLUDED.map_name,
        updated_at     = EXCLUDED.scraped_at,
        source_url     = EXCLUDED.source_url,
        parser_version = EXCLUDED.parser_version
"""

UPSERT_KILL_MATRIX = """
    INSERT INTO kill_matrix (
        match_id, map_number, matrix_type, player1_id, player2_id,
        player1_kills, player2_kills,
        scraped_at, updated_at, source_url, parser_version
    ) VALUES (
        %(match_id)s, %(map_number)s, %(matrix_type)s, %(player1_id)s, %(player2_id)s,
        %(player1_kills)s, %(player2_kills)s,
        %(scraped_at)s, %(scraped_at)s, %(source_url)s, %(parser_version)s
    )
    ON CONFLICT(match_id, map_number, matrix_type, player1_id, player2_id) DO UPDATE SET
        player1_kills  = EXCLUDED.player1_kills,
        player2_kills  = EXCLUDED.player2_kills,
        updated_at     = EXCLUDED.scraped_at,
        source_url     = EXCLUDED.source_url,
        parser_version = EXCLUDED.parser_version
"""

INSERT_QUARANTINE = """
    INSERT INTO quarantine (
        entity_type, match_id, map_number,
        raw_data, error_details, quarantined_at, resolved
    ) VALUES (
        %(entity_type)s, %(match_id)s, %(map_number)s,
        %(raw_data)s, %(error_details)s, %(quarantined_at)s, %(resolved)s
    )
"""


# ---------------------------------------------------------------------------
# Repository class
# ---------------------------------------------------------------------------

class MatchRepository:
    """Data access layer for HLTV match data.

    Wraps all database write and read operations. Receives a psycopg2
    connection. Write methods use transactions for atomicity.
    """

    def __init__(self, conn) -> None:
        self.conn = conn

    def _execute(self, sql, params=None):
        """Execute a single statement within a transaction."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)

    def _executemany(self, sql, params_list):
        """Execute a statement for each param dict within a transaction."""
        with self.conn:
            with self.conn.cursor() as cur:
                for params in params_list:
                    cur.execute(sql, params)

    # ------------------------------------------------------------------
    # Single-row UPSERT methods
    # ------------------------------------------------------------------

    def upsert_match(self, data: dict) -> None:
        self._execute(UPSERT_MATCH, data)

    def upsert_map(self, data: dict) -> None:
        self._execute(UPSERT_MAP, data)

    def upsert_player_stats(self, data: dict) -> None:
        self._execute(UPSERT_PLAYER_STATS, data)

    def upsert_round(self, data: dict) -> None:
        self._execute(UPSERT_ROUND, data)

    def upsert_economy(self, data: dict) -> None:
        self._execute(UPSERT_ECONOMY, data)

    # ------------------------------------------------------------------
    # Batch UPSERT methods (atomic transactions)
    # ------------------------------------------------------------------

    def upsert_match_maps(self, match_data: dict, maps_data: list[dict]) -> None:
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(UPSERT_MATCH, match_data)
                for map_data in maps_data:
                    cur.execute(UPSERT_MAP, map_data)

    def upsert_match_overview(
        self,
        match_data: dict,
        maps_data: list[dict],
        vetoes_data: list[dict],
    ) -> None:
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(UPSERT_MATCH, match_data)
                for map_data in maps_data:
                    cur.execute(UPSERT_MAP, map_data)
                for veto in vetoes_data:
                    cur.execute(UPSERT_VETO, veto)

    def upsert_map_stats_complete(
        self, stats_data: list[dict], rounds_data: list[dict]
    ) -> None:
        with self.conn:
            with self.conn.cursor() as cur:
                for row in stats_data:
                    cur.execute(UPSERT_PLAYER_STATS, row)
                for row in rounds_data:
                    cur.execute(UPSERT_ROUND, row)

    def upsert_map_player_stats(self, stats_data: list[dict]) -> None:
        self._executemany(UPSERT_PLAYER_STATS, stats_data)

    def upsert_map_rounds(self, rounds_data: list[dict]) -> None:
        self._executemany(UPSERT_ROUND, rounds_data)

    def upsert_map_economy(self, economy_data: list[dict]) -> None:
        self._executemany(UPSERT_ECONOMY, economy_data)

    def upsert_kill_matrix(self, data: dict) -> None:
        self._execute(UPSERT_KILL_MATRIX, data)

    def upsert_perf_economy_complete(
        self,
        perf_stats: list[dict],
        economy_data: list[dict],
        kill_matrix_data: list[dict],
    ) -> None:
        with self.conn:
            with self.conn.cursor() as cur:
                for row in perf_stats:
                    cur.execute(UPSERT_PLAYER_STATS, row)
                for row in economy_data:
                    cur.execute(UPSERT_ECONOMY, row)
                for row in kill_matrix_data:
                    cur.execute(UPSERT_KILL_MATRIX, row)

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def _fetchall_dicts(self, sql, params=None) -> list[dict]:
        with self.conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def _fetchone_dict(self, sql, params=None) -> dict | None:
        with self.conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def get_pending_map_stats(self, limit: int = 10) -> list[dict]:
        return self._fetchall_dicts(
            """SELECT m.match_id, m.map_number, m.mapstatsid
               FROM maps m
               WHERE m.mapstatsid IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM player_stats ps
                   WHERE ps.match_id = m.match_id AND ps.map_number = m.map_number
                 )
               ORDER BY m.match_id, m.map_number
               LIMIT %s""",
            (limit,),
        )

    def get_pending_perf_economy(
        self, limit: int = 10, max_attempts: int = 3,
    ) -> list[dict]:
        return self._fetchall_dicts(
            """SELECT m.match_id, m.map_number, m.mapstatsid
               FROM maps m
               WHERE m.mapstatsid IS NOT NULL
                 AND m.perf_attempts < %s
                 AND EXISTS (
                   SELECT 1 FROM player_stats ps
                   WHERE ps.match_id = m.match_id AND ps.map_number = m.map_number
                 )
                 AND EXISTS (
                   SELECT 1 FROM player_stats ps
                   WHERE ps.match_id = m.match_id AND ps.map_number = m.map_number
                     AND ps.kpr IS NULL
                 )
               ORDER BY m.match_id, m.map_number
               LIMIT %s""",
            (max_attempts, limit),
        )

    def increment_perf_attempts(self, match_id: int, map_number: int) -> None:
        self._execute(
            "UPDATE maps SET perf_attempts = perf_attempts + 1 "
            "WHERE match_id = %s AND map_number = %s",
            (match_id, map_number),
        )

    def get_valid_round_numbers(self, match_id: int, map_number: int) -> set[int]:
        rows = self._fetchall_dicts(
            "SELECT round_number FROM round_history "
            "WHERE match_id = %s AND map_number = %s",
            (match_id, map_number),
        )
        return {r["round_number"] for r in rows}

    def get_match(self, match_id: int) -> dict | None:
        return self._fetchone_dict(
            "SELECT * FROM matches WHERE match_id = %s", (match_id,)
        )

    def get_maps(self, match_id: int) -> list[dict]:
        return self._fetchall_dicts(
            "SELECT * FROM maps WHERE match_id = %s ORDER BY map_number",
            (match_id,),
        )

    def get_player_stats(self, match_id: int, map_number: int) -> list[dict]:
        return self._fetchall_dicts(
            "SELECT * FROM player_stats "
            "WHERE match_id = %s AND map_number = %s "
            "ORDER BY player_id",
            (match_id, map_number),
        )

    def get_vetoes(self, match_id: int) -> list[dict]:
        return self._fetchall_dicts(
            "SELECT * FROM vetoes WHERE match_id = %s ORDER BY step_number",
            (match_id,),
        )

    def count_matches(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM matches")
            return cur.fetchone()[0]

    # ------------------------------------------------------------------
    # Quarantine methods
    # ------------------------------------------------------------------

    def insert_quarantine(self, data: dict) -> None:
        self._execute(INSERT_QUARANTINE, data)

    def get_quarantine_count(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM quarantine")
            return cur.fetchone()[0]

    # ------------------------------------------------------------------
    # Atomic persist-all for a complete match
    # ------------------------------------------------------------------

    def persist_complete_match(
        self,
        match_data: dict,
        maps_data: list[dict],
        vetoes_data: list[dict],
        all_stats: list[dict],
        all_rounds: list[dict],
        all_economy: list[dict],
        all_kill_matrix: list[dict],
    ) -> None:
        """Persist all data for a fully-scraped match in one transaction.

        Only called when every stage (overview + map_stats + perf/econ)
        has succeeded for every map.  Nothing is written to the database
        until this method is called, so a failed match leaves zero rows.
        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(UPSERT_MATCH, match_data)
                for row in maps_data:
                    cur.execute(UPSERT_MAP, row)
                for row in vetoes_data:
                    cur.execute(UPSERT_VETO, row)
                for row in all_stats:
                    cur.execute(UPSERT_PLAYER_STATS, row)
                for row in all_rounds:
                    cur.execute(UPSERT_ROUND, row)
                for row in all_economy:
                    cur.execute(UPSERT_ECONOMY, row)
                for row in all_kill_matrix:
                    cur.execute(UPSERT_KILL_MATRIX, row)

    def delete_match_data(self, match_id: int) -> None:
        """Delete all data for a match across all tables."""
        with self.conn:
            with self.conn.cursor() as cur:
                for table in (
                    "economy", "kill_matrix", "round_history",
                    "player_stats", "vetoes", "maps", "matches",
                ):
                    cur.execute(
                        f"DELETE FROM {table} WHERE match_id = %s",
                        (match_id,),
                    )
