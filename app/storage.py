"""Parquet 存储层：版本化代目录 + decimal128 直写，不经浮点。

目录布局：
  /data/parquet/gen-1/<batch_id>.parquet      已提交、不可变的代
  /data/parquet/staging-<migration_id>/...    迁移暂存区，提交前对外不可见

一个代目录里的全部文件共享同一套 decimal128(precision, scale) 模式，
配合 SQLite 活动代指针构成完整快照。
"""
from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .decimal_utils import representable

SAMPLE_FIELD = "样本号"
CONC_FIELD = "浓度"


def decimal_type(precision: int, scale: int) -> pa.Decimal128Type:
    return pa.decimal128(precision, scale)


def build_table(
    samples: List[str],
    concentrations: List[Optional[Decimal]],
    precision: int,
    scale: int,
) -> pa.Table:
    """由 Python str / Decimal 直接构造 Arrow 表，浓度列全程不经 float。"""
    sample_arr = pa.array(samples, type=pa.string())
    conc_arr = pa.array(concentrations, type=decimal_type(precision, scale))
    return pa.table({SAMPLE_FIELD: sample_arr, CONC_FIELD: conc_arr})


def write_parquet(
    path: Path,
    samples: List[str],
    concentrations: List[Optional[Decimal]],
    precision: int,
    scale: int,
) -> int:
    """写单个批次 Parquet，返回行数。先写临时文件再原子 rename，杜绝半文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = build_table(samples, concentrations, precision, scale)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)
    return table.num_rows


def read_parquet(path: Path) -> pa.Table:
    return pq.read_table(path)


def gen_dir(parquet_root: Path, gen_id: int) -> Path:
    d = parquet_root / f"gen-{gen_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def staging_dir(parquet_root: Path, migration_id: int) -> Path:
    d = parquet_root / f"staging-{migration_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def batch_path(parquet_root: Path, gen_id: int, batch_id: str) -> Path:
    return gen_dir(parquet_root, gen_id) / f"{batch_id}.parquet"


def cast_table_to(
    table: pa.Table, precision: int, scale: int
) -> Tuple[pa.Table, List[dict]]:
    """把表的浓度列转型到目标 decimal128(p,s)。

    返回 (新表, 不兼容行列表)。不兼容行依据 representable() 精确定位到样本号，
    不依赖异常，因此可以一次性收集全部问题样本而不是遇到第一个就中断。
    兼容行通过 PyArrow safe cast 转型（零值扩位补零、收窄去零均为精确变换）。
    """
    sample_col = table.column(SAMPLE_FIELD).to_pylist()
    conc_col = table.column(CONC_FIELD).to_pylist()  # Decimal 或 None
    incompatible: List[dict] = []
    for sample, value in zip(sample_col, conc_col):
        if not representable(value, precision, scale):
            incompatible.append(
                {
                    "sample_id": sample,
                    "concentration": None if value is None else str(value),
                    "reason": f"{value} 无法在 decimal128({precision},{scale}) 精确表示",
                }
            )
    if incompatible:
        return None, incompatible
    new_conc = table.column(CONC_FIELD).cast(
        decimal_type(precision, scale), safe=True
    )
    new_table = table.set_column(
        table.schema.get_field_index(CONC_FIELD),
        pa.field(CONC_FIELD, decimal_type(precision, scale)),
        new_conc,
    )
    return new_table, []


def write_table_atomic(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)


def remove_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
