"""Parquet 存储层：版本化代目录 + decimal128 直写，不经浮点。

目录布局：
  /data/parquet/gen-<id>/<batch_id>.parquet      已提交、不可变的代
  /data/parquet/staging-<migration_id>/...       迁移暂存区，提交前对外不可见
  /data/parquet/.trash/<name>-<uuid>/            删除失败的目录/文件隔离区

一个代目录里的全部文件共享同一套 decimal128(precision, scale) 模式，
配合 SQLite 活动代指针构成完整快照。

写盘一律走「唯一临时文件 + fsync + 原子 rename」；删除一律“先尝试直接删，
失败则移动到 .trash 隔离区”，因此清理失败绝不会阻挡重试，也不会碰有效数据。
"""
from __future__ import annotations

import os
import shutil
import uuid
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .decimal_utils import representable

SAMPLE_FIELD = "样本号"
CONC_FIELD = "浓度"

TRASH_DIRNAME = ".trash"
# 所有未完成写盘留下的临时文件后缀（启动恢复时按此前缀清扫）
TMP_SUFFIX = ".tmp"


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


def _tmp_path(path: Path) -> Path:
    """唯一临时文件名：同一路径的并发/重试写盘不会互相覆盖。"""
    return path.with_name(path.name + f".{uuid.uuid4().hex}{TMP_SUFFIX}")


def _fsync_parent(path: Path) -> None:
    try:
        dir_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_parquet(
    path: Path,
    samples: List[str],
    concentrations: List[Optional[Decimal]],
    precision: int,
    scale: int,
) -> int:
    """写单个批次 Parquet，返回行数。

    临时文件 + fsync + 原子 rename。写盘失败只可能留下 .tmp 文件，
    绝不会出现半成品覆盖正式文件。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = build_table(samples, concentrations, precision, scale)
    tmp = _tmp_path(path)
    try:
        pq.write_table(table, tmp, compression="snappy")
        # 落盘后再 rename，崩溃也不会留下内容残缺的正式文件
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_parent(path)
    except Exception:
        # 写盘失败：尽力清理本次临时文件；清理再失败也不掩盖原始写盘错误
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
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
    """与 write_parquet 同口径的原子写，用于迁移暂存区重写。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        pq.write_table(table, tmp, compression="snappy")
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_parent(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def trash_root(parquet_root: Path) -> Path:
    d = parquet_root / TRASH_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _quarantine(path: Path, parquet_root: Optional[Path]) -> Path:
    """删除失败时把对象移入 .trash 隔离区，保证不阻挡后续重试。"""
    root = parquet_root or path.parent
    target = trash_root(root) / f"{path.name}-{uuid.uuid4().hex}"
    shutil.move(str(path), str(target))
    return target


def remove_dir(path: Path, parquet_root: Optional[Path] = None) -> bool:
    """删除目录；直接删除失败时隔离到 .trash，再失败只记录不抛出。

    返回 True 表示目录已不存在（删除或隔离成功）。任何情况下都不触碰
    其它代目录，有效数据不受影响。
    """
    if not path.exists() and not path.is_symlink():
        return True
    try:
        shutil.rmtree(path)
        return True
    except OSError:
        try:
            _quarantine(path, parquet_root)
            return True
        except OSError:
            return False


def remove_file(path: Path, parquet_root: Optional[Path] = None) -> bool:
    """删除单个文件；失败时隔离，绝不抛给调用方演变成数据损坏。"""
    if not path.exists():
        return True
    try:
        path.unlink()
        return True
    except OSError:
        try:
            _quarantine(path, parquet_root)
            return True
        except OSError:
            return False


def cleanup_tmp_files(root: Path) -> int:
    """清扫 root 下全部未完成写盘的 .tmp 文件（崩溃恢复用），返回清理数量。"""
    removed = 0
    if not root.exists():
        return 0
    for tmp in root.rglob(f"*{TMP_SUFFIX}"):
        if remove_file(tmp, root):
            removed += 1
    return removed


def is_gen_dir(path: Path) -> bool:
    return path.is_dir() and path.name.startswith("gen-")


def is_staging_dir(path: Path) -> bool:
    return path.is_dir() and path.name.startswith("staging-")


def parquet_files(path: Path) -> List[Path]:
    if not path.exists():
        return []
    return sorted(p for p in path.iterdir() if p.is_file() and p.suffix == ".parquet")


def verify_parquet_file(path: Path, precision: int, scale: int) -> None:
    """打开并校验单个 Parquet：可读、浓度列模式与声明一致。失败抛异常。"""
    table = pq.read_table(path)
    field = table.schema.field(CONC_FIELD)
    expected = decimal_type(precision, scale)
    if field.type != expected:
        raise RuntimeError(
            f"文件 {path.name} 浓度列模式为 {field.type}，期望 {expected}"
        )


def verify_gen_complete(
    gen_path: Path, expected_filenames: List[str], precision: int, scale: int
) -> List[str]:
    """校验代目录完整：登记的每个文件都存在、可读且模式正确，无残缺/异类型文件。

    返回问题描述列表（空列表代表完整一致）。
    """
    problems: List[str] = []
    if not gen_path.is_dir():
        return [f"代目录不存在: {gen_path}"]
    present = {p.name for p in parquet_files(gen_path)}
    for name in expected_filenames:
        if name not in present:
            problems.append(f"缺少文件: {name}")
            continue
        try:
            verify_parquet_file(gen_path / name, precision, scale)
        except Exception as exc:  # 可读/模式校验失败
            problems.append(f"文件 {name} 校验失败: {exc!r}")
    extra = present - set(expected_filenames)
    for name in sorted(extra):
        problems.append(f"存在未登记文件: {name}")
    # 任何残留临时文件都视为不完整
    for tmp in gen_path.glob(f"*{TMP_SUFFIX}"):
        problems.append(f"存在未完成写盘的临时文件: {tmp.name}")
    return problems
