"""
Baseline store — persists and retrieves historical performance samples.

Two backends:
  JSONStore   — flat JSON file per metric; good for local dev and git-committed baselines
  SQLiteStore — single SQLite DB; good for CI where many parallel workers write results

Both implement BaselineStore ABC so callers are backend-agnostic.

Usage:
    store = BaselineStore.from_config(helix_config)
    store.record("fio_rand_read_iops", 52_000.0)
    history = store.get_samples("fio_rand_read_iops", limit=5)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Sequence

logger = logging.getLogger(__name__)


class BaselineStore(ABC):
    """Abstract baseline store. Implementations provide record/get_samples/clear."""

    def __init__(self) -> None:
        self._update_mode: bool = False

    def set_update_mode(self, enabled: bool) -> None:
        """
        When update_mode=True, record() stores new samples but comparisons still run.
        Used with --update-baselines pytest flag after intentional performance changes.
        """
        self._update_mode = enabled

    @property
    def update_mode(self) -> bool:
        return self._update_mode

    @abstractmethod
    def record(self, metric: str, value: float, run_id: str | None = None) -> None:
        """Persist a new sample for the given metric."""

    @abstractmethod
    def get_samples(self, metric: str, limit: int = 5) -> list[float]:
        """Return the most recent `limit` samples, oldest-first."""

    @abstractmethod
    def clear(self, metric: str) -> None:
        """Delete all samples for a metric (e.g., after intentional regressions are accepted)."""

    @classmethod
    def from_config(cls, config: object) -> "BaselineStore":
        """
        Factory: reads helix_config.baseline_backend ('json' | 'sqlite').
        Defaults to JSONStore with baselines/ directory.
        """
        backend = getattr(config, "baseline_backend", "json")
        if backend == "sqlite":
            db_path = getattr(config, "baseline_db_path", "baselines/helix_baselines.db")
            return SQLiteStore(db_path)
        baselines_dir = getattr(config, "baseline_dir", "baselines")
        return JSONStore(baselines_dir)


# ─── JSON backend ─────────────────────────────────────────────────────────────

class JSONStore(BaselineStore):
    """
    Flat JSON files: one file per metric (e.g., baselines/fio_rand_read_iops.json).
    Each file: {"samples": [{"value": float, "timestamp": float, "run_id": str}]}

    Git-committable: baseline drift is visible in PR diffs.
    """

    def __init__(self, baselines_dir: str | Path = "baselines") -> None:
        super().__init__()
        self._dir = Path(baselines_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, metric: str) -> Path:
        safe_name = metric.replace("/", "_").replace(" ", "_")
        return self._dir / f"{safe_name}.json"

    def record(self, metric: str, value: float, run_id: str | None = None) -> None:
        with self._lock:
            path = self._path(metric)
            data = self._load(path)
            data["samples"].append({
                "value": value,
                "timestamp": time.time(),
                "run_id": run_id or "unknown",
            })
            path.write_text(json.dumps(data, indent=2))
        logger.debug("JSONStore: recorded %s = %.3f", metric, value)

    def get_samples(self, metric: str, limit: int = 5) -> list[float]:
        path = self._path(metric)
        if not path.exists():
            return []
        data = self._load(path)
        samples = data.get("samples", [])
        recent = samples[-limit:]
        return [s["value"] for s in recent]

    def clear(self, metric: str) -> None:
        path = self._path(metric)
        if path.exists():
            path.unlink()
        logger.info("JSONStore: cleared baseline for %s", metric)

    def _load(self, path: Path) -> dict:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                logger.warning("JSONStore: corrupt file %s — resetting", path)
        return {"samples": []}


# ─── SQLite backend ────────────────────────────────────────────────────────────

class SQLiteStore(BaselineStore):
    """
    Single SQLite database for CI environments with multiple workers.
    Thread-safe via connection-per-thread + WAL mode.
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS samples (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        metric    TEXT    NOT NULL,
        value     REAL    NOT NULL,
        timestamp REAL    NOT NULL,
        run_id    TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_metric_ts ON samples (metric, timestamp);
    """

    def __init__(self, db_path: str | Path = "baselines/helix_baselines.db") -> None:
        super().__init__()
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # Initialize schema
        self._conn().executescript(self._SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(str(self._db_path), timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return self._local.conn

    def record(self, metric: str, value: float, run_id: str | None = None) -> None:
        self._conn().execute(
            "INSERT INTO samples (metric, value, timestamp, run_id) VALUES (?, ?, ?, ?)",
            (metric, value, time.time(), run_id or "unknown"),
        )
        self._conn().commit()
        logger.debug("SQLiteStore: recorded %s = %.3f", metric, value)

    def get_samples(self, metric: str, limit: int = 5) -> list[float]:
        cursor = self._conn().execute(
            "SELECT value FROM samples WHERE metric = ? ORDER BY timestamp DESC LIMIT ?",
            (metric, limit),
        )
        rows = cursor.fetchall()
        # Return oldest-first (reversed since we fetched DESC)
        return [row[0] for row in reversed(rows)]

    def clear(self, metric: str) -> None:
        self._conn().execute("DELETE FROM samples WHERE metric = ?", (metric,))
        self._conn().commit()
        logger.info("SQLiteStore: cleared baseline for %s", metric)
