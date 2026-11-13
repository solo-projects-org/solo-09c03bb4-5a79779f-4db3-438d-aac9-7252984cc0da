"""SQLite 登记层：批次元数据、活动代指针、迁移记录。

并发模型（单实例、单进程、多线程 uvicorn worker）：
- 所有 DB 访问经由一把进程内 RLock 串行化；SQLite 自身开启 WAL。
- 活动代指针 + 代目录是不可变快照，读路径在同一把锁内取指针并枚举该代文件，
  保证每次查询/下载只看到一套完整、自洽的模式与批次。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS active_generation (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    gen_id      INTEGER NOT NULL,
    precision   INTEGER NOT NULL,
    scale       INTEGER NOT NULL,
    activated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT NOT NULL,
    gen_id      INTEGER NOT NULL,
    filename    TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (gen_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_gen ON batches(gen_id);

CREATE TABLE IF NOT EXISTS migrations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    status          TEXT NOT NULL CHECK (status IN ('running','committed','failed')),
    target_precision INTEGER NOT NULL,
    target_scale    INTEGER NOT NULL,
    staging_gen_id  INTEGER NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    detail          TEXT
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Catalog:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> "Catalog":
        """建立连接并建表；幂等。需在数据目录已创建后调用。"""
        if self._conn is not None:
            return self
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA)
        return self

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def closed(self) -> bool:
        return self._conn is None

    def _check_open(self) -> None:
        if self._conn is None:
            raise RuntimeError("Catalog 已关闭")

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ---------------- 活动代 ----------------

    def get_active(self) -> Dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT gen_id, precision, scale, activated_at FROM active_generation WHERE id = 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("活动代尚未初始化")
            return dict(row)

    def set_active(self, gen_id: int, precision: int, scale: int) -> None:
        """调用方必须已持有锁（迁移提交事务内调用）。"""
        self._conn.execute(
            "INSERT INTO active_generation(id, gen_id, precision, scale, activated_at) "
            "VALUES (1, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET gen_id=excluded.gen_id, "
            "precision=excluded.precision, scale=excluded.scale, activated_at=excluded.activated_at",
            (gen_id, precision, scale, utcnow()),
        )

    # ---------------- 批次 ----------------

    def add_batch(
        self, batch_id: str, gen_id: int, filename: str, row_count: int
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO batches(batch_id, gen_id, filename, row_count, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (batch_id, gen_id, filename, row_count, utcnow()),
            )

    def list_batches(self, gen_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if gen_id is None:
                rows = self._conn.execute(
                    "SELECT batch_id, gen_id, filename, row_count, created_at "
                    "FROM batches ORDER BY created_at, batch_id"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT batch_id, gen_id, filename, row_count, created_at "
                    "FROM batches WHERE gen_id = ? ORDER BY created_at, batch_id",
                    (gen_id,),
                ).fetchall()
            return [dict(r) for r in rows]

    def count_batches(self, gen_id: int) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM batches WHERE gen_id = ?", (gen_id,)
            ).fetchone()[0]

    # ---------------- 迁移记录 ----------------

    def start_migration(
        self, target_precision: int, target_scale: int, staging_gen_id: int
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO migrations(status, target_precision, target_scale, "
                "staging_gen_id, started_at) VALUES ('running', ?, ?, ?, ?)",
                (target_precision, target_scale, staging_gen_id, utcnow()),
            )
            return int(cur.lastrowid)

    def finish_migration(
        self, migration_id: int, status: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE migrations SET status=?, finished_at=?, detail=? WHERE id=?",
                (status, utcnow(), json.dumps(detail, ensure_ascii=False) if detail else None, migration_id),
            )

    def get_running_migration(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, status, target_precision, target_scale, staging_gen_id, "
                "started_at, finished_at, detail FROM migrations WHERE status='running' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def get_migration(self, migration_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, status, target_precision, target_scale, staging_gen_id, "
                "started_at, finished_at, detail FROM migrations WHERE id=?",
                (migration_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_migrations(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, status, target_precision, target_scale, staging_gen_id, "
                "started_at, finished_at, detail FROM migrations ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_running_failed_on_restart(self) -> List[int]:
        """启动恢复：把所有遗留的 running 迁移标记为 failed（半成品永不暴露）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM migrations WHERE status='running'"
            ).fetchall()
            ids = [int(r["id"]) for r in rows]
            for mid in ids:
                self._conn.execute(
                    "UPDATE migrations SET status='failed', finished_at=?, "
                    "detail=? WHERE id=?",
                    (
                        utcnow(),
                        json.dumps(
                            {"reason": "进程重启，未完成的迁移判定为失败"},
                            ensure_ascii=False,
                        ),
                        mid,
                    ),
                )
            return ids

    # ---------------- 迁移提交事务 ----------------

    def commit_migration(
        self,
        migration_id: int,
        new_gen_id: int,
        precision: int,
        scale: int,
        copied_batches: List[Dict[str, Any]],
    ) -> None:
        """原子提交：一个事务内登记新代全部批次、翻转活动代、迁移置 committed。

        调用方必须已持有独占提交锁。任一语句失败则整体回滚，活动代保持不变，
        绝不会出现半迁移结果对外可见。
        """
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                for b in copied_batches:
                    self._conn.execute(
                        "INSERT INTO batches(batch_id, gen_id, filename, row_count, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            b["batch_id"],
                            new_gen_id,
                            b["filename"],
                            b["row_count"],
                            b["created_at"],
                        ),
                    )
                self.set_active(new_gen_id, precision, scale)
                self._conn.execute(
                    "UPDATE migrations SET status='committed', finished_at=?, detail=? WHERE id=?",
                    (
                        utcnow(),
                        json.dumps(
                            {"new_gen_id": new_gen_id, "batch_count": len(copied_batches)},
                            ensure_ascii=False,
                        ),
                        migration_id,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise


def create_catalog(db_path: Path) -> "Catalog":
    """工厂：创建并连接登记库（含建表），便于单实例装配与测试隔离。"""
    return Catalog(db_path).connect()
