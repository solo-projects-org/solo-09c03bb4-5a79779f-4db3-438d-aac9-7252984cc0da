"""SQLite 登记层：批次元数据、活动代指针、迁移记录（含两阶段提交相位）。

并发模型（单实例、单进程、多线程 uvicorn worker）：
- 所有 DB 访问经由一把进程内 RLock 串行化；SQLite 自身开启 WAL。
- 活动代指针 + 代目录是不可变快照，读路径在同一把锁内取指针并枚举该代文件，
  保证每次查询/下载只看到一套完整、自洽的模式与批次。

迁移相位（migrations.phase）
----------------------------
preparing  暂存区重写中（活动代指针仍是旧代）
publishing 登记库事务已提交：新代批次已登记、活动指针已翻转，目录提升待确认
committed  目录提升并校验完成，迁移彻底成功
failed     失败或重启时未完成，活动代保持/恢复为旧的完整代

phases 与 status 的关系：status='running' 时 phase ∈ {preparing, publishing}；
status='committed' 时 phase='committed'；status='failed' 时 phase='failed'。
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
    phase           TEXT NOT NULL DEFAULT 'preparing'
                    CHECK (phase IN ('preparing','publishing','committed','failed')),
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
        self._migrate_legacy_schema()
        return self

    def _migrate_legacy_schema(self) -> None:
        """老版本库没有 phase 列时补齐（默认 preparing，与旧 running 语义一致）。"""
        cols = {
            r["name"]
            for r in self._conn.execute("PRAGMA table_info(migrations)").fetchall()
        }
        if "phase" not in cols:
            self._conn.execute(
                "ALTER TABLE migrations ADD COLUMN phase TEXT NOT NULL "
                "DEFAULT 'preparing' CHECK "
                "(phase IN ('preparing','publishing','committed','failed'))"
            )

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

    def delete_batches_for_gen(self, gen_id: int) -> int:
        """恢复 publishing 迁移时删除误登记到新代的批次，返回删除行数。"""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM batches WHERE gen_id = ?", (gen_id,)
            )
            return cur.rowcount

    # ---------------- 迁移记录 ----------------

    def start_migration(
        self, target_precision: int, target_scale: int, staging_gen_id: int
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO migrations(status, phase, target_precision, target_scale, "
                "staging_gen_id, started_at) VALUES ('running', 'preparing', ?, ?, ?, ?)",
                (target_precision, target_scale, staging_gen_id, utcnow()),
            )
            return int(cur.lastrowid)

    def mark_phase_publishing(self, migration_id: int) -> None:
        """事务内调用：记录已进入发布相位（目录提升前）。"""
        self._conn.execute(
            "UPDATE migrations SET phase='publishing' WHERE id=?", (migration_id,)
        )

    def mark_committed(self, migration_id: int, detail: Dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE migrations SET status='committed', phase='committed', "
                "finished_at=?, detail=? WHERE id=?",
                (utcnow(), json.dumps(detail, ensure_ascii=False), migration_id),
            )

    def finish_migration(
        self, migration_id: int, status: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        with self._lock:
            phase = "committed" if status == "committed" else "failed"
            self._conn.execute(
                "UPDATE migrations SET status=?, phase=?, finished_at=?, detail=? WHERE id=?",
                (
                    status,
                    phase,
                    utcnow(),
                    json.dumps(detail, ensure_ascii=False) if detail else None,
                    migration_id,
                ),
            )

    def get_running_migration(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, status, phase, target_precision, target_scale, "
                "staging_gen_id, started_at, finished_at, detail FROM migrations "
                "WHERE status='running' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def get_migration(self, migration_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, status, phase, target_precision, target_scale, "
                "staging_gen_id, started_at, finished_at, detail FROM migrations WHERE id=?",
                (migration_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_migrations(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, status, phase, target_precision, target_scale, "
                "staging_gen_id, started_at, finished_at, detail FROM migrations "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_unfinished_migrations(self) -> List[Dict[str, Any]]:
        """启动恢复用：列出所有 status='running' 的迁移（含 preparing/publishing）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, status, phase, target_precision, target_scale, "
                "staging_gen_id, started_at, finished_at, detail FROM migrations "
                "WHERE status='running' ORDER BY id"
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_failed_with_detail(self, migration_id: int, detail: Dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE migrations SET status='failed', phase='failed', "
                "finished_at=?, detail=? WHERE id=?",
                (utcnow(), json.dumps(detail, ensure_ascii=False), migration_id),
            )

    # ---------------- 迁移提交事务 ----------------

    def commit_migration(
        self,
        migration_id: int,
        new_gen_id: int,
        precision: int,
        scale: int,
        copied_batches: List[Dict[str, Any]],
    ) -> None:
        """原子提交（两阶段提交的第一阶段，仅写库）：

        一个事务内登记新代全部批次、翻转活动代、迁移置 publishing。
        任一语句失败则整体回滚，活动代保持旧代，绝不会出现半迁移结果对外可见。
        事务提交成功只代表“登记层已发布”，物理目录提升由调用方在事务外完成，
        之后再把相位推进到 committed。
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
                self.mark_phase_publishing(migration_id)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise


def create_catalog(db_path: Path) -> "Catalog":
    """工厂：创建并连接登记库（含建表），便于单实例装配与测试隔离。"""
    return Catalog(db_path).connect()
