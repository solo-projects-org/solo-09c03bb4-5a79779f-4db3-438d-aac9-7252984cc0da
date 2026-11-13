"""故障注入器：在迁移/上传的关键阶段制造写盘错误、登记失败与进程退出。

仅用于本地演示与测试（网页控制台与 /api/admin/faults 可配置）。
生产环境将 WATER_FAULT_ENABLED=0 即完全关闭，所有 inject() 调用变为空操作。

注入点（point）
---------------
staging_write   : 迁移暂存区某个批次 Parquet 写盘前（模拟磁盘写错误）
db_commit       : 登记库提交翻转事务前（模拟登记失败）
promote         : 暂存目录切换为正式代目录前（模拟切换失败）
promote_exit    : 切换瞬间直接退出进程（模拟切换时被杀，需重启恢复）
staging_cleanup : 失败后清理暂存目录时（模拟清理再次失败，验证不损伤有效数据）
upload_write    : 上传批次写盘前
upload_register : 上传批次写盘成功、登记前（模拟登记失败回滚）
upload_cleanup  : 上传登记失败后回滚删除文件时（模拟回滚清理失败）

触发方式
--------
mode=raise  : 抛出 RuntimeError（业务流程捕获后走失败/回滚分支）
mode=exit   : 以 os._exit 立即终止进程（模拟 kill -9 / 断电，不执行任何清理）
every=N     : 每第 N 次命中该注入点触发一次（默认 1，即首次命中即触发）
prob=0..1   : 按概率触发（与 every 取“或”）
one_shot=1  : 触发一次后自动关闭（默认开启，避免污染后续请求）
"""
from __future__ import annotations

import os
import random
import threading
from typing import Any, Callable, Dict, Optional

VALID_POINTS = {
    "staging_write",
    "db_commit",
    "promote",
    "promote_exit",
    "staging_cleanup",
    "upload_write",
    "upload_register",
    "upload_cleanup",
}


class HardExitError(RuntimeError):
    """测试模式下替代 os._exit 的异常（exit_func 被替换时使用）。"""


class FaultInjector:
    def __init__(
        self,
        enabled: bool = True,
        point: Optional[str] = None,
        mode: str = "raise",
        every: int = 1,
        prob: float = 0.0,
        one_shot: bool = True,
        exit_func: Optional[Callable[[int], None]] = None,
    ) -> None:
        self._lock = threading.RLock()
        self.enabled = enabled
        self.point = point if point in VALID_POINTS else None
        self.mode = mode if mode in ("raise", "exit") else "raise"
        self.every = max(1, int(every))
        self.prob = min(1.0, max(0.0, float(prob)))
        self.one_shot = one_shot
        self._hits: Dict[str, int] = {p: 0 for p in VALID_POINTS}
        self._fires: Dict[str, int] = {p: 0 for p in VALID_POINTS}
        # 测试可替换为抛异常的函数，避免杀死 pytest 进程
        self._exit_func = exit_func or (lambda code: os._exit(code))

    @classmethod
    def from_env(cls) -> "FaultInjector":
        def truthy(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None or raw == "":
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            enabled=truthy("WATER_FAULT_ENABLED", True),
            point=os.environ.get("WATER_FAULT_POINT") or None,
            mode=os.environ.get("WATER_FAULT_MODE", "raise"),
            every=int(os.environ.get("WATER_FAULT_EVERY", "1") or "1"),
            prob=float(os.environ.get("WATER_FAULT_PROB", "0") or "0"),
            one_shot=truthy("WATER_FAULT_ONE_SHOT", True),
        )

    def configure(self, **kwargs: Any) -> Dict[str, Any]:
        with self._lock:
            if "enabled" in kwargs:
                self.enabled = bool(kwargs["enabled"])
            if "point" in kwargs:
                point = kwargs["point"]
                self.point = point if point in VALID_POINTS else None
            if "mode" in kwargs and kwargs["mode"] in ("raise", "exit"):
                self.mode = kwargs["mode"]
            if "every" in kwargs:
                self.every = max(1, int(kwargs["every"]))
            if "prob" in kwargs:
                self.prob = min(1.0, max(0.0, float(kwargs["prob"])))
            if "one_shot" in kwargs:
                self.one_shot = bool(kwargs["one_shot"])
            return self.status()

    def reset(self) -> None:
        with self._lock:
            self.point = None
            self.prob = 0.0
            self._hits = {p: 0 for p in VALID_POINTS}
            self._fires = {p: 0 for p in VALID_POINTS}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "point": self.point,
                "mode": self.mode,
                "every": self.every,
                "prob": self.prob,
                "one_shot": self.one_shot,
                "hits": dict(self._hits),
                "fires": dict(self._fires),
                "valid_points": sorted(VALID_POINTS),
            }

    def inject(self, point: str, detail: str = "") -> None:
        """命中注入点时按配置抛错或退出；未配置则空操作。"""
        if point not in VALID_POINTS:
            raise ValueError(f"未知故障注入点: {point}")
        with self._lock:
            if not self.enabled or self.point != point:
                return
            self._hits[point] += 1
            hit = self._hits[point]
            should_fire = (hit % self.every == 0) or (
                self.prob > 0 and random.random() < self.prob
            )
            if not should_fire:
                return
            self._fires[point] += 1
            mode = self.mode
            if self.one_shot:
                # 触发后立即解除，保证后续正常请求不再被注入
                self.point = None
        message = f"[故障注入] 注入点 {point} 触发（mode={mode}）{('：' + detail) if detail else ''}"
        if mode == "exit":
            # 立即终止，不执行 finally/清理，模拟断电或 kill -9
            self._exit_func(2)
            raise HardExitError(message)  # exit_func 被测试替换时的兜底
        raise RuntimeError(message)
