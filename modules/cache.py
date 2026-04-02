import sqlite3
import json
import time
from config import DB_PATH


class CacheDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    expires_at INTEGER NOT NULL
                )
            """)
            conn.commit()

    def get(self, key: str):
        """Return cached value or None if missing/expired."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload, expires_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        if int(time.time()) > row["expires_at"]:
            self.invalidate(key)
            return None
        return json.loads(row["payload"])

    def set(self, key: str, value, ttl_hours: float) -> None:
        expires_at = int(time.time()) + int(ttl_hours * 3600)
        payload = json.dumps(value)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, payload, expires_at) VALUES (?, ?, ?)",
                (key, payload, expires_at),
            )
            conn.commit()

    def invalidate(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()

    def clear_expired(self) -> int:
        """Delete all expired entries. Returns count removed."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM cache WHERE expires_at <= ?", (int(time.time()),)
            )
            conn.commit()
            return cursor.rowcount

    def clear_all(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cache")
            conn.commit()

    def last_refreshed(self, key: str) -> int | None:
        """Return the expires_at timestamp for a key (or None)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT expires_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        return row["expires_at"] if row else None


# Singleton instance used across the app
cache = CacheDB()
