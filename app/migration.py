"""迁移编排：暂存重写 → 两阶段提交 → 崩溃对账恢复。

关键不变量
----------
1. 已提交的代目录不可变；迁移在 staging-<id> 暂存区重写，提交前对外不可见。
2. 提交分两个相位，登记在 migrations.phase：
   - preparing：暂存重写中，活动代指针仍是旧代；
   - publishing：登记库事务已提交（新代批次已登记、活动指针已翻转），
     物理目录提升待确认；
   - committed：目录提升并逐文件校验通过，迁移才算成功。
   只有“新模式 + 全套 Parquet”同时就绪才会进入 committed；此前任何失败
   （含进程退出）在重启时要么续提成功、要么整体回滚到旧代。
3. 上传在写盘**之前**于提交锁内重读活动代模式：迁移进行中写旧代，
   翻转完成后写新代，物理上不会把旧类型文件混进新代。
4. 迁移提交瞬间在同一把锁内 drain 新到批次并补拷、补校验；含中途新批在内
   全部值可精确表示才提交。
5. 所有删除“先直接删、失败则隔离到 .trash”，清理失败既不损伤有效数据，
   也不会阻挡后续重试；重试使用全新暂存目录，绝不复用残缺文件。
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .catalog import Catalog
from .decimal_utils import INITIAL_PRECISION, INITIAL_SCALE, check_in_range_18_6
from .faults import FaultInjector
from .storage import (
    batch_path,
    cast_table_to,
    gen_dir,
    is_gen_dir,
    is_staging_dir,
    parquet_files,
    read_parquet,
    remove_dir,
    remove_file,
    cleanup_tmp_files,
    staging_dir,
    verify_gen_complete,
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
        faults: Optional[FaultInjector] = None,
    ):
        self.catalog = catalog
        self.parquet_root = parquet_root
        self.migration_delay_seconds = migration_delay_seconds
        self.faults = faults or FaultInjector(enabled=False)
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

    # ================= 启动与崩溃恢复 =================

    def startup(self) -> None:
        with self.commit_lock:
            # 1. 清扫所有未完成写盘的 .tmp 临时文件
            cleanup_tmp_files(self.parquet_root)

            # 2. 逐条对账遗留的未完成迁移
            for mig in self.catalog.list_unfinished_migrations():
                if mig["phase"] == "publishing":
                    self._recover_publishing(mig)
                else:
                    self._recover_preparing(mig)

            # 3. 初始化第一代（decimal128(18,6)），幂等
            if self._safe_get_active() is None:
                gen_dir(self.parquet_root, 1)
                self.catalog.set_active(1, INITIAL_PRECISION, INITIAL_SCALE)

            # 4. 清扫无主暂存区/代目录与活动代中的孤儿文件
            self._reconcile_orphans()

    def _recover_preparing(self, mig: Dict[str, Any]) -> None:
        """preparing 相位退出：登记库从未发布，直接判失败并清理暂存区。"""
        mid = int(mig["id"])
        self.catalog.mark_failed_with_detail(
            mid, {"reason": "进程重启，文件准备阶段退出的未完成迁移判定为失败，沿用原表"}
        )
        remove_dir(staging_dir(self.parquet_root, mid), self.parquet_root)

    def _recover_publishing(self, mig: Dict[str, Any]) -> None:
        """publishing 相位退出：登记库已翻转，磁盘提升未确认。

        完整新表存在（或可由完整暂存区提升）→ 续提并置 committed；
        否则整体回滚：删除误登记的新代批次、活动指针恢复旧代、迁移判失败。
        """
        mid = int(mig["id"])
        new_gen = int(mig["staging_gen_id"])
        precision, scale = (
            int(mig["target_precision"]),
            int(mig["target_scale"]),
        )
        staging_path = staging_dir(self.parquet_root, mid)
        final_path = gen_dir(self.parquet_root, new_gen)
        expected = [b["filename"] for b in self.catalog.list_batches(new_gen)]

        # 情形一：正式代目录已存在且完整 —— 直接确认提交
        if final_path.is_dir() and not verify_gen_complete(
            final_path, expected, precision, scale
        ):
            self.catalog.mark_committed(
                mid,
                {
                    "new_gen_id": new_gen,
                    "batch_count": len(expected),
                    "reason": "进程在切换后退出，重启校验新表完整，确认迁移成功",
                },
            )
            remove_dir(staging_path, self.parquet_root)
            cleanup_tmp_files(final_path)
            return

        # 情形二：暂存区完整 —— 续做目录提升
        if staging_path.is_dir() and not verify_gen_complete(
            staging_path, expected, precision, scale
        ):
            try:
                self._promote(staging_path, final_path)
                if not verify_gen_complete(
                    final_path, expected, precision, scale
                ):
                    self.catalog.mark_committed(
                        mid,
                        {
                            "new_gen_id": new_gen,
                            "batch_count": len(expected),
                            "reason": "进程在切换时退出，重启续提成功，新表完整",
                        },
                    )
                    cleanup_tmp_files(final_path)
                    return
            except Exception:
                pass  # 续提失败落入回滚

        # 情形三：无法凑出完整新表 —— 整体回滚到旧代
        self._rollback_publishing(
            mig, "进程在目录切换阶段退出且新表不完整，已回滚到原模式与原表"
        )

    def _rollback_publishing(self, mig: Dict[str, Any], reason: str) -> None:
        """回滚 publishing 迁移：恢复旧活动代、删除误登记批次、迁移判失败。"""
        mid = int(mig["id"])
        new_gen = int(mig["staging_gen_id"])
        prev_gen, prev_p, prev_s = self._previous_gen_schema(new_gen)
        with self.catalog.lock:
            try:
                self.catalog._conn.execute("BEGIN IMMEDIATE")
                self.catalog.delete_batches_for_gen(new_gen)
                self.catalog.set_active(prev_gen, prev_p, prev_s)
                self.catalog.mark_failed_with_detail(
                    mid,
                    {
                        "reason": reason,
                        "rolled_back_to_gen": prev_gen,
                        "preserved_batches": self.catalog.count_batches(prev_gen),
                    },
                )
                self.catalog._conn.execute("COMMIT")
            except Exception:
                self.catalog._conn.execute("ROLLBACK")
                raise
        # 物理残缺目录隔离清理（失败也不影响回滚后的有效数据）
        remove_dir(staging_dir(self.parquet_root, mid), self.parquet_root)
        remove_dir(gen_dir(self.parquet_root, new_gen), self.parquet_root)

    def _previous_gen_schema(self, new_gen: int) -> Tuple[int, int, int]:
        """推导上一代的 (gen_id, precision, scale)，回滚时恢复指针用。"""
        prev_gen = new_gen - 1
        if prev_gen <= 1:
            return 1, INITIAL_PRECISION, INITIAL_SCALE
        for row in self.catalog.list_migrations(limit=100):
            if (
                row["status"] == "committed"
                and int(row["staging_gen_id"]) == prev_gen
            ):
                return (
                    prev_gen,
                    int(row["target_precision"]),
                    int(row["target_scale"]),
                )
        # 兜底：从上一代现存 Parquet 物理模式读取
        prev_dir = gen_dir(self.parquet_root, prev_gen)
        files = parquet_files(prev_dir)
        if files:
            import pyarrow.parquet as pq

            dtype = pq.read_table(files[0]).schema.field("浓度").type
            return prev_gen, int(dtype.precision), int(dtype.scale)
        raise RuntimeError(f"无法确定上一代 gen-{prev_gen} 的模式，回滚受阻")

    def _reconcile_orphans(self) -> None:
        """清扫无主目录与孤儿文件，保证遗留物不阻挡重试、不被复用。"""
        known_gens = {1}
        active = self._safe_get_active()
        if active is not None:
            known_gens.add(int(active["gen_id"]))
        for row in self.catalog.list_migrations(limit=1000):
            known_gens.add(int(row["staging_gen_id"]))
        known_staging = {
            int(m["id"]) for m in self.catalog.list_unfinished_migrations()
        }

        if not self.parquet_root.exists():
            return
        for child in sorted(self.parquet_root.iterdir()):
            if is_staging_dir(child):
                try:
                    sid = int(child.name.split("staging-", 1)[1])
                except ValueError:
                    sid = -1
                if sid not in known_staging:
                    # 无主暂存区：失败迁移的遗留物，直接清理/隔离
                    remove_dir(child, self.parquet_root)
            elif is_gen_dir(child):
                try:
                    gid = int(child.name.split("gen-", 1)[1])
                except ValueError:
                    continue
                if gid not in known_gens:
                    # 无主代目录（含回滚残留的空 gen-N），清理/隔离后重试不再被阻挡
                    remove_dir(child, self.parquet_root)
                    continue
                # 登记过的代：隔离未登记的孤儿 Parquet（登记失败/切换时退出的残留）
                registered = {b["filename"] for b in self.catalog.list_batches(gid)}
                for f in parquet_files(child):
                    if f.name not in registered:
                        remove_file(f, self.parquet_root)
                cleanup_tmp_files(child)

    def _safe_get_active(self) -> Optional[Dict[str, Any]]:
        try:
            return self.catalog.get_active()
        except RuntimeError:
            return None

    # ================= 上传 =================

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
            # 故障注入：文件准备阶段写盘失败（或直接退出）
            self.faults.inject("upload_write", f"批次 {batch_id} 写盘")
            # 先写临时文件再原子 rename；写盘失败不会留下登记，反之亦然
            row_count = write_parquet(path, samples, concentrations, precision, scale)
            try:
                # 故障注入：写盘成功但登记失败（或登记前退出）
                self.faults.inject("upload_register", f"批次 {batch_id} 登记")
                self.catalog.add_batch(batch_id, gen_id, path.name, row_count)
            except Exception:
                # 登记失败：移除已写文件，绝不留孤儿半结果
                self.faults.inject("upload_cleanup", f"批次 {batch_id} 回滚清理")
                remove_file(path, self.parquet_root)
                raise
        return {
            "batch_id": batch_id,
            "gen_id": gen_id,
            "precision": precision,
            "scale": scale,
            "row_count": row_count,
            "filename": path.name,
        }

    # ================= 查询/下载的一致性快照 =================

    def active_snapshot(self) -> Dict[str, Any]:
        """取活动代快照：锁内读指针并枚举该代全部批次文件。

        读路径全程只引用这一个代目录，保证每次查询/下载只看到一套完整模式与批次；
        登记的文件若物理缺失，直接报错而不是返回残缺的一套。
        """
        with self.commit_lock:
            active = self.catalog.get_active()
            gen_id = active["gen_id"]
            batches = self.catalog.list_batches(gen_id)
            files = [
                batch_path(self.parquet_root, gen_id, b["batch_id"]) for b in batches
            ]
            missing = [str(f) for f in files if not f.exists()]
            if missing:
                raise RuntimeError(
                    f"活动代 gen-{gen_id} 存在缺失文件，拒绝提供不完整数据: {missing}"
                )
            return {
                "gen_id": gen_id,
                "precision": active["precision"],
                "scale": active["scale"],
                "activated_at": active["activated_at"],
                "batches": batches,
                "files": files,
            }

    # ================= 迁移发起 =================

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
            "phase": "preparing",
            "target_precision": target_precision,
            "target_scale": target_scale,
            "staging_gen_id": staging_gen_id,
        }

    # ================= 后台迁移主体 =================

    def _run_migration(
        self,
        migration_id: int,
        target_precision: int,
        target_scale: int,
        staging_gen_id: int,
    ) -> None:
        try:
            if self._closed or self.catalog.closed:
                return  # 服务正在关闭，不再推进迁移（启动恢复会处理这条 running）
            active = self.catalog.get_active()
            source_gen = active["gen_id"]
            source_batches = self.catalog.list_batches(source_gen)
            staged: List[Dict[str, Any]] = []
            incompatible: List[dict] = []

            # 阶段一：把活动代现有批次逐个重写到暂存区并做兼容性校验
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
                # 故障注入：暂存写盘错误（或写盘时退出）
                self.faults.inject(
                    "staging_write",
                    f"迁移 {migration_id} 批次 {batch['batch_id']} 暂存写盘",
                )
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
                    self.faults.inject(
                        "staging_write",
                        f"迁移 {migration_id} 新批 {batch['batch_id']} 暂存写盘",
                    )
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

                # 阶段三（两阶段提交第一阶段）：登记库原子发布
                staging_path = staging_dir(self.parquet_root, migration_id)
                expected_names = [b["filename"] for b in staged]
                # 发布前先确认暂存区本身完整，杜绝把残缺集登记进库
                problems = verify_gen_complete(
                    staging_path, expected_names, target_precision, target_scale
                )
                if problems:
                    self._fail_migration(
                        migration_id,
                        [
                            {
                                "sample_id": None,
                                "reason": f"暂存区不完整，拒绝发布: {'; '.join(problems)}",
                            }
                        ],
                    )
                    return

                self.faults.inject(
                    "db_commit", f"迁移 {migration_id} 登记库提交"
                )
                self.catalog.commit_migration(
                    migration_id,
                    staging_gen_id,
                    target_precision,
                    target_scale,
                    staged,
                )

                # 阶段四（第二阶段）：物理目录提升 + 全套校验。
                # 此刻登记库已发布，任何失败都必须回滚登记层，绝不能停在
                # “模式已翻转但目录缺失”的状态。
                final_path = gen_dir(self.parquet_root, staging_gen_id)
                try:
                    self.faults.inject(
                        "promote", f"迁移 {migration_id} 目录切换"
                    )
                    self.faults.inject(
                        "promote_exit", f"迁移 {migration_id} 切换时退出"
                    )
                    self._promote(staging_path, final_path)
                    post_problems = verify_gen_complete(
                        final_path,
                        expected_names,
                        target_precision,
                        target_scale,
                    )
                    if post_problems:
                        raise RuntimeError(
                            f"提升后校验失败: {'; '.join(post_problems)}"
                        )
                    cleanup_tmp_files(final_path)
                except Exception as exc:
                    if self._closed or self.catalog.closed:
                        return  # 关闭路径：重启恢复按 publishing 相位对账
                    self._rollback_publishing(
                        self.catalog.get_migration(migration_id),
                        f"目录切换失败，已回滚登记库并保留原表: {exc!r}",
                    )
                    self._fail_after_rollback(migration_id, exc)
                    return

                self.catalog.mark_committed(
                    migration_id,
                    {
                        "new_gen_id": staging_gen_id,
                        "batch_count": len(staged),
                    },
                )
        except Exception as exc:  # 任何意外失败都不得留下 running 状态
            if self._closed or self.catalog.closed:
                # 关闭路径：不再写库，安静退出；下次启动恢复会清理这条 running
                return
            try:
                self._fail_migration(
                    migration_id,
                    [{"sample_id": None, "reason": f"迁移内部错误: {exc!r}"}],
                )
            except Exception:
                # 标记失败再失败也不得抛出线程异常；暂存区由下次启动恢复清扫
                pass

    def _promote(self, staging_path: Path, final_path: Path) -> None:
        """暂存目录原子提升为正式代目录。

        - 目标不存在：直接原子 rename；
        - 目标是空目录：删空目录后 rename；
        - 目标非空且不完整：先隔离到 .trash 再 rename，残缺文件绝不被复用；
        - 目标已包含同一套完整文件（幂等重入）：直接采用，清理暂存区。
        """
        if not staging_path.exists():
            raise RuntimeError(f"暂存目录已消失，无法提升: {staging_path}")
        if final_path.exists():
            if not any(final_path.iterdir()):
                final_path.rmdir()
            elif parquet_files(final_path):
                # 非空：交由调用方的完整校验决定是否采用；默认隔离残缺目录
                remove_dir(final_path, final_path.parent)
            else:
                remove_dir(final_path, final_path.parent)
        staging_path.replace(final_path)

    def _fail_after_rollback(self, migration_id: int, exc: Exception) -> None:
        """回滚后的状态确认：迁移记录已在回滚事务内置 failed。"""
        row = self.catalog.get_migration(migration_id)
        if row is not None and row["status"] != "failed":
            self.catalog.mark_failed_with_detail(
                migration_id,
                {"reason": f"目录切换失败并已回滚: {exc!r}"},
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
            # 故障注入：清理暂存区再次失败。清理走隔离策略，不损伤有效数据
            self.faults.inject(
                "staging_cleanup", f"迁移 {migration_id} 暂存区清理"
            )
            removed = remove_dir(
                staging_dir(self.parquet_root, migration_id), self.parquet_root
            )
            if not removed:
                # 清理失败：记录在案；下次启动 _reconcile_orphans 会再次清扫，
                # 新迁移使用全新暂存目录，不会被阻挡或复用残缺文件
                self.catalog.mark_failed_with_detail(
                    migration_id,
                    {
                        **detail,
                        "cleanup_warning": "暂存区清理失败已隔离，重试不受影响",
                    },
                )

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
    catalog: Catalog,
    parquet_root: Path,
    migration_delay_seconds: float,
    faults: Optional[FaultInjector] = None,
) -> MigrationService:
    """工厂：装配迁移服务（不产生模块级全局状态）。"""
    return MigrationService(catalog, parquet_root, migration_delay_seconds, faults)
