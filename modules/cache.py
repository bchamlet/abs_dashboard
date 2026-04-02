import os
import sqlite3
import json
import time
import pandas as pd
from config import DB_PATH

_DATA_DIR = os.path.dirname(DB_PATH)


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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dataset_catalog (
                    key TEXT PRIMARY KEY,
                    dataflow_id TEXT NOT NULL,
                    dataflow_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    start_period TEXT NOT NULL,
                    end_period TEXT NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    parquet_path TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    is_warm_cache INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()

    # ------------------------------------------------------------------
    # JSON cache (metadata: structures, dataflows)
    # ------------------------------------------------------------------

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
        # Remove all Parquet files tracked in the catalog
        with self._connect() as conn:
            rows = conn.execute("SELECT parquet_path FROM dataset_catalog").fetchall()
        for row in rows:
            path = row["parquet_path"]
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        with self._connect() as conn:
            conn.execute("DELETE FROM dataset_catalog")
            conn.execute("DELETE FROM cache")
            conn.commit()

    def last_refreshed(self, key: str) -> int | None:
        """Return the expires_at timestamp for a key (or None)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT expires_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        return row["expires_at"] if row else None

    # ------------------------------------------------------------------
    # Parquet cache (observation DataFrames)
    # ------------------------------------------------------------------

    def get_df(self, key: str) -> pd.DataFrame | None:
        """Return cached DataFrame from Parquet, or None if missing/expired."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT parquet_path, expires_at FROM dataset_catalog WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        if int(time.time()) > row["expires_at"]:
            self.catalog_invalidate(key)
            return None
        path = row["parquet_path"]
        if not os.path.exists(path):
            return None
        return pd.read_parquet(path)

    def set_df(self, key: str, df: pd.DataFrame, ttl_hours: float, meta: dict) -> None:
        """Write DataFrame to Parquet and record metadata in dataset_catalog.

        meta keys: dataflow_id, dataflow_name, version, start_period, end_period, is_warm_cache
        Invalidates any existing catalog entry for the same dataflow first so
        only one Parquet file exists per dataset at a time.
        """
        self._catalog_invalidate_by_dataflow(meta["dataflow_id"], meta["version"])
        filename = (
            f"{meta['dataflow_id']}_{meta['version']}_"
            f"{meta['start_period']}_{meta['end_period']}.parquet"
        )
        path = os.path.join(_DATA_DIR, filename)
        df.to_parquet(path, index=False)
        expires_at = int(time.time()) + int(ttl_hours * 3600)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO dataset_catalog
                (key, dataflow_id, dataflow_name, version, start_period, end_period,
                 fetched_at, row_count, parquet_path, expires_at, is_warm_cache)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    meta["dataflow_id"],
                    meta["dataflow_name"],
                    meta["version"],
                    meta["start_period"],
                    meta["end_period"],
                    int(time.time()),
                    len(df),
                    path,
                    expires_at,
                    int(meta.get("is_warm_cache", 0)),
                ),
            )
            conn.commit()

    def catalog_get_by_dataflow(self, dataflow_id: str, version: str) -> dict | None:
        """Return the catalog entry for a given dataflow/version, or None.
        Does not filter by expiry — used only to recover the stored date range."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dataset_catalog WHERE dataflow_id = ? AND version = ? LIMIT 1",
                (dataflow_id, version),
            ).fetchone()
        return dict(row) if row else None

    def catalog_list(self) -> list[dict]:
        """Return all catalog entries as a list of dicts, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM dataset_catalog ORDER BY fetched_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def catalog_invalidate(self, key: str) -> None:
        """Delete a catalog entry and its Parquet file."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT parquet_path FROM dataset_catalog WHERE key = ?", (key,)
            ).fetchone()
        if row:
            path = row["parquet_path"]
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        with self._connect() as conn:
            conn.execute("DELETE FROM dataset_catalog WHERE key = ?", (key,))
            conn.commit()

    def _catalog_invalidate_by_dataflow(self, dataflow_id: str, version: str) -> None:
        """Remove all catalog entries and files for a given dataflow/version."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, parquet_path FROM dataset_catalog WHERE dataflow_id = ? AND version = ?",
                (dataflow_id, version),
            ).fetchall()
        for row in rows:
            path = row["parquet_path"]
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM dataset_catalog WHERE dataflow_id = ? AND version = ?",
                (dataflow_id, version),
            )
            conn.commit()


# Singleton instance used across the app
cache = CacheDB()
