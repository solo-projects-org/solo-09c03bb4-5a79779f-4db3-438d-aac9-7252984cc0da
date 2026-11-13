"""应用配置，全部通过环境变量覆盖，默认值面向 Docker 数据卷。"""
from __future__ import annotations

import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    def __init__(self) -> None:
        # 数据卷根目录：SQLite 登记库与所有 Parquet 代目录都在其下
        self.DATA_DIR = Path(os.environ.get("WATER_DATA_DIR", "/data"))
        # 演示用：迁移时每个批次重写之间的停顿秒数，便于在网页上演示中途上传
        self.MIGRATION_DELAY_SECONDS = float(
            os.environ.get("MIGRATION_DELAY_SECONDS", "0.3")
        )
        # 单个上传 CSV 的大小上限（字节），默认 16 MiB
        self.MAX_UPLOAD_BYTES = _int_env(
            "MAX_UPLOAD_BYTES", 16 * 1024 * 1024
        )
        # 故障注入开关：演示/测试环境默认开启，生产设 WATER_FAULT_ENABLED=0 关闭
        self.FAULT_ENABLED = _bool_env("WATER_FAULT_ENABLED", True)

    @property
    def db_path(self) -> Path:
        return self.DATA_DIR / "catalog.db"

    @property
    def parquet_root(self) -> Path:
        return self.DATA_DIR / "parquet"


settings = Settings()
