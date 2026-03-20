"""
SQLite database layer for persistent storage.

Tables:
  - resolved_markets  : past resolved esports markets for repeat-offender detection
  - alert_history     : 24h dedup for Discord alerts
  - liquipedia_cache  : cached Liquipedia data with TTL
  - tournament_fingerprints : tournament metadata refreshed every 6h
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import config
from logger import log


class Database:
    """SQLite database manager for the anomaly bot."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        if self.db_path != ":memory:":
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._init_tables()

    def _connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        log.info(f"Database connected: {self.db_path}")

    def _init_tables(self):
        """Create tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS resolved_markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT UNIQUE NOT NULL,
                slug TEXT,
                question TEXT,
                team_a TEXT,
                team_b TEXT,
                tournament TEXT,
                game TEXT,
                outcome TEXT,
                resolved_at TEXT,
                was_fifty_fifty INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_resolved_tournament
                ON resolved_markets(tournament);
            CREATE INDEX IF NOT EXISTS idx_resolved_teams
                ON resolved_markets(team_a, team_b);

            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                alert_hash TEXT,
                score REAL,
                recommendation TEXT,
                flags_json TEXT,
                alerted_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_alert_market
                ON alert_history(market_id);
            CREATE INDEX IF NOT EXISTS idx_alert_time
                ON alert_history(alerted_at);

            CREATE TABLE IF NOT EXISTS liquipedia_cache (
                cache_key TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                fetched_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tournament_fingerprints (
                tournament_key TEXT PRIMARY KEY,
                game TEXT,
                tournament_name TEXT,
                matches_json TEXT,
                teams_json TEXT,
                fetched_at REAL NOT NULL
            );
        """)
        self.conn.commit()
        log.debug("Database tables initialized")

    # ─── Resolved Markets ───────────────────────────────────────────────

    def add_resolved_market(
        self,
        market_id: str,
        slug: str = "",
        question: str = "",
        team_a: str = "",
        team_b: str = "",
        tournament: str = "",
        game: str = "",
        outcome: str = "",
        resolved_at: str = "",
        was_fifty_fifty: bool = False,
    ):
        """Store a resolved market for repeat-offender tracking."""
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO resolved_markets
                   (market_id, slug, question, team_a, team_b, tournament,
                    game, outcome, resolved_at, was_fifty_fifty)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (market_id, slug, question, team_a, team_b, tournament,
                 game, outcome, resolved_at, int(was_fifty_fifty)),
            )
            self.conn.commit()
            log.debug(f"Stored resolved market: {market_id}")
        except sqlite3.Error as e:
            log.error(f"Failed to store resolved market {market_id}: {e}")

    def get_resolved_markets_for_tournament(self, tournament: str) -> List[Dict]:
        """Get all resolved markets for a tournament (for repeat-offender filter)."""
        cursor = self.conn.execute(
            """SELECT * FROM resolved_markets
               WHERE tournament = ? ORDER BY resolved_at DESC""",
            (tournament,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_fifty_fifty_markets_for_tournament(self, tournament: str) -> List[Dict]:
        """Get markets that resolved 50/50 for a tournament."""
        cursor = self.conn.execute(
            """SELECT * FROM resolved_markets
               WHERE tournament = ? AND was_fifty_fifty = 1
               ORDER BY resolved_at DESC""",
            (tournament,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_all_resolved_team_names(self) -> List[str]:
        """Get all team names from resolved markets (for pattern matching)."""
        cursor = self.conn.execute(
            "SELECT DISTINCT team_a FROM resolved_markets "
            "UNION SELECT DISTINCT team_b FROM resolved_markets"
        )
        return [row[0] for row in cursor.fetchall() if row[0]]

    # ─── Alert History ──────────────────────────────────────────────────

    def is_alert_sent_recently(self, market_id: str, hours: int = None) -> bool:
        """Check if an alert was sent for this market within the dedup window."""
        hours = hours or config.DEDUP_HOURS
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM alert_history WHERE market_id = ? AND alerted_at > ?",
            (market_id, cutoff),
        )
        count = cursor.fetchone()[0]
        return count > 0

    def record_alert(
        self,
        market_id: str,
        alert_hash: str = "",
        score: float = 0.0,
        recommendation: str = "",
        flags: List[Dict] = None,
    ):
        """Record that an alert was sent."""
        try:
            self.conn.execute(
                """INSERT INTO alert_history
                   (market_id, alert_hash, score, recommendation, flags_json)
                   VALUES (?, ?, ?, ?, ?)""",
                (market_id, alert_hash, score, recommendation,
                 json.dumps(flags or [])),
            )
            self.conn.commit()
            log.debug(f"Recorded alert for market: {market_id}")
        except sqlite3.Error as e:
            log.error(f"Failed to record alert for {market_id}: {e}")

    def cleanup_old_alerts(self, hours: int = None):
        """Remove alerts older than the dedup window."""
        hours = hours or config.DEDUP_HOURS * 2  # Keep double the window
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            self.conn.execute(
                "DELETE FROM alert_history WHERE alerted_at < ?", (cutoff,)
            )
            self.conn.commit()
            log.debug("Cleaned up old alerts")
        except sqlite3.Error as e:
            log.error(f"Failed to cleanup alerts: {e}")

    # ─── Liquipedia Cache ───────────────────────────────────────────────

    def get_cache(self, cache_key: str, ttl: int = None) -> Optional[Any]:
        """Retrieve cached data if not expired."""
        ttl = ttl or config.LIQUIPEDIA_CACHE_TTL
        cursor = self.conn.execute(
            "SELECT data_json, fetched_at FROM liquipedia_cache WHERE cache_key = ?",
            (cache_key,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        if time.time() - row["fetched_at"] > ttl:
            return None  # Expired
        try:
            return json.loads(row["data_json"])
        except json.JSONDecodeError:
            return None

    def set_cache(self, cache_key: str, data: Any):
        """Store data in cache."""
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO liquipedia_cache
                   (cache_key, data_json, fetched_at)
                   VALUES (?, ?, ?)""",
                (cache_key, json.dumps(data), time.time()),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Failed to cache {cache_key}: {e}")

    # ─── Tournament Fingerprints ────────────────────────────────────────

    def get_tournament_fingerprint(self, tournament_key: str) -> Optional[Dict]:
        """Get cached tournament fingerprint."""
        cursor = self.conn.execute(
            "SELECT * FROM tournament_fingerprints WHERE tournament_key = ?",
            (tournament_key,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        if time.time() - row["fetched_at"] > config.TOURNAMENT_REFRESH_INTERVAL:
            return None  # Expired
        return {
            "tournament_key": row["tournament_key"],
            "game": row["game"],
            "tournament_name": row["tournament_name"],
            "matches": json.loads(row["matches_json"] or "[]"),
            "teams": json.loads(row["teams_json"] or "[]"),
            "fetched_at": row["fetched_at"],
        }

    def set_tournament_fingerprint(
        self,
        tournament_key: str,
        game: str,
        tournament_name: str,
        matches: List[Dict],
        teams: List[str],
    ):
        """Cache a tournament fingerprint."""
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO tournament_fingerprints
                   (tournament_key, game, tournament_name, matches_json, teams_json, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (tournament_key, game, tournament_name,
                 json.dumps(matches), json.dumps(teams), time.time()),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Failed to cache tournament {tournament_key}: {e}")

    # ─── Utility ────────────────────────────────────────────────────────

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            log.info("Database connection closed")
