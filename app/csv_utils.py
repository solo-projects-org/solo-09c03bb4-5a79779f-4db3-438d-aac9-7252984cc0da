"""CSV 解析：仅接受“样本号、浓度”两列，浓度走 Decimal，空值保留。"""
from __future__ import annotations

import csv
import io
from decimal import Decimal
from typing import List, Optional, Tuple

from .decimal_utils import DecimalParseError, parse_concentration
from .storage import CONC_FIELD, SAMPLE_FIELD

# 允许的表头别名（首个匹配即采用），兼容省库里常见列名写法
SAMPLE_HEADER_ALIASES = {SAMPLE_FIELD, "样本号", "sample_id", "sample", "样本编号"}
CONC_HEADER_ALIASES = {CONC_FIELD, "浓度", "concentration", "conc"}


class CsvValidationError(ValueError):
    pass


def parse_csv(content: bytes) -> Tuple[List[str], List[Optional[Decimal]]]:
    """解析上传的 CSV 字节，返回 (样本号列表, 浓度 Decimal/None 列表)。

    - UTF-8（含 BOM）解码
    - 必须且只能包含样本号、浓度两列（顺序不限）
    - 浓度空单元格保留为 None；非空必须能解析为有限 Decimal
    - 样本号不得为空
    """
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV 必须为 UTF-8 编码") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("CSV 内容为空，缺少表头") from exc

    header_norm = [h.strip().strip("﻿") for h in header]
    sample_idx = conc_idx = None
    for i, name in enumerate(header_norm):
        if name in SAMPLE_HEADER_ALIASES and sample_idx is None:
            sample_idx = i
        elif name in CONC_HEADER_ALIASES and conc_idx is None:
            conc_idx = i
    if sample_idx is None or conc_idx is None:
        raise CsvValidationError(
            f"表头必须包含样本号与浓度两列，实际为: {header_norm}"
        )
    if len(header_norm) != 2:
        raise CsvValidationError(
            f"只允许两列（样本号、浓度），实际 {len(header_norm)} 列: {header_norm}"
        )

    samples: List[str] = []
    concentrations: List[Optional[Decimal]] = []
    for line_no, row in enumerate(reader, start=2):
        if not row or all((c is None or c.strip() == "") for c in row):
            continue  # 跳过完全空白行
        if len(row) != 2:
            raise CsvValidationError(f"第 {line_no} 行列数不是 2: {row}")
        sample = row[sample_idx].strip()
        if sample == "":
            raise CsvValidationError(f"第 {line_no} 行样本号为空")
        raw_conc = row[conc_idx]
        try:
            value = parse_concentration(raw_conc)
        except DecimalParseError as exc:
            raise CsvValidationError(f"第 {line_no} 行浓度无效：{exc}") from exc
        samples.append(sample)
        concentrations.append(value)

    if not samples:
        raise CsvValidationError("CSV 没有任何有效数据行")
    return samples, concentrations
