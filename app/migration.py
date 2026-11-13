"""迁移编排：后台重写全部批次、提交时全量兼容性校验、原子翻转活动代。

关键不变量
----------
1. 已提交的代目录不可变；迁移在 staging-<id> 暂存区重写，提交前对外不可见。
2. 活动代翻转只在 commit_migration() 的单个 SQLite 事务内发生，失败整体回滚，
   写盘或登记失败都不会暴露半迁移结果。
3. 上传在写盘**之前**于提交锁内重读活动代模式：
   - 迁移进行中（活动代未翻转）-> 按旧模式写入旧代；
   - 翻转完成后才走到写盘 -> 按新模式写入新代。
   因此“切换前开始、切换后完成”的上传绝不会把旧类型文件混进新代。
4. 迁移提交瞬间在同一把锁内做 drain：把提交期间到达、写入旧代的新批补拷进
   暂存代并逐一做兼容性校验。含中途新批在内的全部值都可精确表示才提交。
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from .catalog import Catalog
from .decimal_utils import INITIAL_PRECISION, INITIAL_SCALE, check_in_range_18_6
from .storage import (
    batch_path,
    cast_table_to,
    gen_dir,
    read_parquet,
    remove_dir,
    staging_dir,
    write_parquet,
    write_table_atomic,
)


class MigrationRunningError(RuntimeError):
    """已有迁移在运行，拒绝再次发起。"""


class MigrationService:
    def __init__(
        self,
        catalog: Catalog,
        parquet_root: Path,
        migration_delay_seconds: float = 0.0,
    ):
        self.catalog = catalog
        self.parquet_root = parquet_root
        self.migration_delay_seconds = migration_delay_seconds
        # 协调“迁移独占提交”与“上传收尾”的同一把可重入锁
        self.commit_lock = threading.RLock()
        # 正在运行的迁移线程，供关闭时等待，避免 daemon 线程在连接关闭后触库
        self._migration_threads: List[threading.Thread] = []
        self._closed = False

    def shutdown(self, timeout: float = 10.0) -> None:
        """等待在途迁移线程结束后再返回，保证关闭过程不产生游离后台写。"""
        self._closed = True
        for thread in list(self._migration_threads):
            if thread.is_alive():
                thread.join(timeout=timeout)
        self._migration_threads.clear()

    # ---------------- 启动与恢复 ----------------

    def startup(self) -> None:
        with self.commit_lock:
            # 崩溃恢复：重启后未完成的迁移一律判失败，暂存区清掉
            stale_ids = self.catalog.mark_running_failed_on_restart()
            for mid in stale_ids:
                remove_dir(staging_dir(self.parquet_root, mid))
            # 初始化第一代（decimal128(18,6)），幂等
            if self._safe_get_active() is None:
                gen_dir(self.parquet_root, 1)
                self.catalog.set_active(1, INITIAL_PRECISION, INITIAL_SCALE)

    def _safe_get_active(self) -> Optional[Dict[str, Any]]:
        try:
            return self.catalog.get_active()
        except RuntimeError:
            return None

    # ---------------- 上传 ----------------

    def upload_batch(
        self, samples: List[str], concentrations: List[Optional[Decimal]]
    ) -> Dict[str, Any]:
        """登记一个新批次。写盘前在锁内重读活动代，保证模式始终与活动代一致。"""
        for value in concentrations:
            check_in_range_18_6(value)
        batch_id = (
            datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:10]
        )

        with self.commit_lock:
            active = self.catalog.get_active()
            gen_id, precision, scale = (
                active["gen_id"],
                active["precision"],
                active["scale"],
            )
            path = batch_path(self.parquet_root, gen_id, batch_id)
            # 先写临时文件再原子 rename；写盘失败不会留下登记，反之亦然
            row_count = write_parquet(path, samples, concentrations, precision, scale)
            try:
                self.catalog.add_batch(batch_id, gen_id, path.name, row_count)
            except Exception:
                # 登记失败：移除已写文件，绝不留孤儿半结果
                path.unlink(missing_ok=True)
                raise
        return {
            "batch_id": batch_id,
            "gen_id": gen_id,
            "precision": precision,
            "scale": scale,
            "row_count": row_count,
            "filename": path.name,
        }

    # ---------------- 查询/下载的一致性快照 ----------------

    def active_snapshot(self) -> Dict[str, Any]:
        """取活动代快照：锁内读指针并枚举该代全部批次文件。

        读路径全程只引用这一个代目录，保证每次查询/下载只看到一套完整模式与批次。
        """
        with self.commit_lock:
            active = self.catalog.get_active()
            gen_id = active["gen_id"]
            batches = self.catalog.list_batches(gen_id)
            files = [
                batch_path(self.parquet_root, gen_id, b["batch_id"]) for b in batches
            ]
            return {
                "gen_id": gen_id,
                "precision": active["precision"],
                "scale": active["scale"],
                "activated_at": active["activated_at"],
                "batches": batches,
                "files": files,
            }

    # ---------------- 迁移发起 ----------------

    def start_migration(
        self, target_precision: int, target_scale: int
    ) -> Dict[str, Any]:
        if not (1 <= target_scale <= target_precision <= 38):
            raise ValueError("目标精度需满足 1 <= 小数位 <= 总位数 <= 38")
        with self.commit_lock:
            if self.catalog.get_running_migration() is not None:
                raise MigrationRunningError("已有迁移正在运行，同一时间只允许一次迁移")
            active = self.catalog.get_active()
            if (
                active["precision"] == target_precision
                and active["scale"] == target_scale
            ):
                raise ValueError("目标模式与当前活动模式相同，无需迁移")
            staging_gen_id = active["gen_id"] + 1
            migration_id = self.catalog.start_migration(
                target_precision, target_scale, staging_gen_id
            )
            staging_dir(self.parquet_root, migration_id)  # 预建暂存区

        thread = threading.Thread(
            target=self._run_migration,
            args=(migration_id, target_precision, target_scale, staging_gen_id),
            name=f"migration-{migration_id}",
            daemon=True,
        )
        # 清理已结束线程的引用，再登记新线程
        self._migration_threads = [t for t in self._migration_threads if t.is_alive()]
        self._migration_threads.append(thread)
        thread.start()
        return {
            "migration_id": migration_id,
            "status": "running",
            "target_precision": target_precision,
            "target_scale": target_scale,
            "staging_gen_id": staging_gen_id,
        }

    # ---------------- 后台迁移主体 ----------------

    def _run_migration(
        self,
        migration_id: int,
        target_precision: int,
        target_scale: int,
        staging_gen_id: int,
    ) -> None:
        try:
            if self._closed or self.catalog.closed:
                return  # 服务正在关闭，不再推进迁移（启动恢复会将其判失败）
            active = self.catalog.get_active()
            source_gen = active["gen_id"]
            source_batches = self.catalog.list_batches(source_gen)
            staged: List[Dict[str, Any]] = []
            incompatible: List[dict] = []

            # 阶段一：把活动代的现有批次逐个重写到暂存区并做兼容性校验
            for batch in source_batches:
                src = batch_path(self.parquet_root, source_gen, batch["batch_id"])
                table = read_parquet(src)
                new_table, bad = cast_table_to(
                    table, target_precision, target_scale
                )
                if bad:
                    for item in bad:
                        item["batch_id"] = batch["batch_id"]
                    incompatible.extend(bad)
                    continue
                dst = staging_dir(self.parquet_root, migration_id) / src.name
                write_table_atomic(new_table, dst)
                staged.append(
                    {
                        "batch_id": batch["batch_id"],
                        "filename": src.name,
                        "row_count": batch["row_count"],
                        "created_at": batch["created_at"],
                    }
                )
                if self.migration_delay_seconds > 0:
                    time.sleep(self.migration_delay_seconds)

            if incompatible:
                self._fail_migration(migration_id, incompatible)
                return

            # 阶段二：独占提交。锁内 drain 提交期间到达的新批并再次全量校验
            with self.commit_lock:
                current = self.catalog.get_active()
                # 防御性检查：活动代在本迁移期间不应被别人改动
                if current["gen_id"] != source_gen:
                    self._fail_migration(
                        migration_id,
                        [{"sample_id": None, "reason": "活动代在迁移期间发生变化"}],
                    )
                    return

                staged_ids = {b["batch_id"] for b in staged}
                current_batches = self.catalog.list_batches(source_gen)
                new_arrivals = [
                    b for b in current_batches if b["batch_id"] not in staged_ids
                ]
                for batch in new_arrivals:
                    src = batch_path(
                        self.parquet_root, source_gen, batch["batch_id"]
                    )
                    table = read_parquet(src)
                    new_table, bad = cast_table_to(
                        table, target_precision, target_scale
                    )
                    if bad:
                        for item in bad:
                            item["batch_id"] = batch["batch_id"]
                        incompatible.extend(bad)
                        continue
                    dst = staging_dir(self.parquet_root, migration_id) / src.name
                    write_table_atomic(new_table, dst)
                    staged.append(
                        {
                            "batch_id": batch["batch_id"],
                            "filename": src.name,
                            "row_count": batch["row_count"],
                            "created_at": batch["created_at"],
                        }
                    )

                if incompatible:
                    self._fail_migration(migration_id, incompatible)
                    return

                # 全部值（含中途新批）均可精确表示：单事务原子翻转
                self.catalog.commit_migration(
                    migration_id,
                    staging_gen_id,
                    target_precision,
                    target_scale,
                    staged,
                )
                # 提交成功后暂存目录升级为正式代目录。
                # gen_dir() 可能已预建空目录，先删空目录再原子 rename。
                staging_path = staging_dir(self.parquet_root, migration_id)
                final_path = gen_dir(self.parquet_root, staging_gen_id)
                if final_path.exists():
                    if not any(final_path.iterdir()):
                        final_path.rmdir()
                    else:
                        raise RuntimeError(
                            f"目标代目录 {final_path} 已存在且非空，拒绝覆盖"
                        )
                staging_path.replace(final_path)
        except Exception as exc:  # 任何意外失败都不得留下 running 状态
            if self._closed or self.catalog.closed:
                # 关闭路径：不再写库，安静退出；下次启动恢复会清理这条 running
                return
            self._fail_migration(
                migration_id,
                [{"sample_id": None, "reason": f"迁移内部错误: {exc!r}"}],
            )

    def _fail_migration(
        self,
        migration_id: int,
        incompatible: List[dict],
    ) -> None:
        if self._closed or self.catalog.closed:
            return  # 关闭中不写库；暂存目录由下次启动恢复清理
        if incompatible and all(
            "sample_id" in item and "reason" in item for item in incompatible
        ) and any(item.get("sample_id") is not None for item in incompatible):
            detail = {
                "reason": "存在无法在目标精度下精确表示的浓度，迁移失败；原表与已接收新批均保留",
                "incompatible_samples": incompatible,
                "incompatible_count": len(incompatible),
            }
        else:
            detail = {
                "reason": "迁移失败，原表与已接收新批均保留",
                "incompatible_samples": incompatible,
                "incompatible_count": len(incompatible),
            }
        with self.commit_lock:
            self.catalog.finish_migration(migration_id, "failed", detail)
            remove_dir(staging_dir(self.parquet_root, migration_id))

    def migration_status(self, migration_id: int) -> Optional[Dict[str, Any]]:
        row = self.catalog.get_migration(migration_id)
        return self._present(row) if row else None

    def current_migration(self) -> Optional[Dict[str, Any]]:
        row = self.catalog.get_running_migration()
        return self._present(row) if row else None

    def list_migrations(self) -> List[Dict[str, Any]]:
        return [self._present(r) for r in self.catalog.list_migrations()]

    @staticmethod
    def _present(row: Dict[str, Any]) -> Dict[str, Any]:
        import json

        out = dict(row)
        if out.get("detail"):
            try:
                out["detail"] = json.loads(out["detail"])
            except (ValueError, TypeError):
                pass
        return out


def create_migration_service(
    catalog: Catalog, parquet_root: Path, migration_delay_seconds: float
) -> MigrationService:
    """工厂：装配迁移服务（不产生模块级全局状态）。"""
    return MigrationService(catalog, parquet_root, migration_delay_seconds)
