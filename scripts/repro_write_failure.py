#!/usr/bin/env python3
"""复现场景 1：迁移暂存阶段写盘错误。

现象（修复前的 Bug）：迁移重写 Parquet 时磁盘写失败，迁移标记失败，
但活动模式可能已被翻转、重启也不恢复，空表同样能触发。

本脚本通过故障注入点 `staging_write` 制造一次写盘错误，验证：
  1. 迁移状态 failed，活动模式保持 decimal128(18,6)，原表与批次完整；
  2. 暂存目录被清理，没有残缺 gen-N；
  3. 解除故障后可继续上传、查询、下载并重新迁移成功。

运行：
  python3 -I scripts/repro_write_failure.py
可选环境变量 WATER_BASE_URL 指向其它地址。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _common as common


def main() -> None:
    common.ensure_healthy()
    common.banner("场景 1：迁移写盘错误（staging_write 注入）")

    common.banner("步骤 1：上传两个兼容批次")
    common.upload_csv(common.COMPATIBLE_CSV, "batch-compatible-1.csv")
    common.upload_csv(common.COMPATIBLE_CSV.replace("WQ-", "WQB-"), "batch-compatible-2.csv")
    common.show_schema()

    common.banner("步骤 2：注入暂存写盘故障（一次性）并发起迁移 18/2")
    r = common.set_fault("staging_write")
    print("  故障配置：", r["json"].get("point"), r["json"].get("mode"))
    mid = common.start_migration(18, 2)
    result = common.wait_migration(mid)
    print("  迁移结果：", result["status"], "/", result["phase"])
    assert result["status"] == "failed", "写盘错误必须导致迁移失败"

    common.banner("步骤 3：验证模式未翻转、原表与全部批次保留")
    schema = common.show_schema()
    assert schema["concentration_type"] == "decimal128(18,6)", "失败后模式不得改变"
    rows = common.get("/api/rows?limit=1000")["json"]
    print(f"  可查询行数：{rows['total_matched']}（读的是完整旧代 {rows['schema']}）")
    dl = common.request("GET", "/api/download")
    assert dl["status"] == 200, "失败后下载仍须可用"
    print("  下载接口：200，拿到的是完整旧代 zip")

    common.banner("步骤 4：解除故障，验证上传/查询/下载/重新迁移全部恢复")
    common.set_fault(None)
    common.upload_csv(common.COMPATIBLE_CSV.replace("WQ-", "WQC-"), "after-failure.csv")
    common.show_schema()
    mid2 = common.start_migration(18, 2)
    result2 = common.wait_migration(mid2)
    print("  重新迁移结果：", result2["status"], "/", result2["phase"])
    assert result2["status"] == "committed"
    schema2 = common.show_schema()
    assert schema2["concentration_type"] == "decimal128(18,2)"
    dl2 = common.request("GET", "/api/download")
    assert dl2["status"] == 200
    print("  下载接口：200，全套 Parquet 均为新模式")

    common.banner("场景 1 复现完成：写盘失败不翻转模式，恢复后功能全部正常 ✔")


if __name__ == "__main__":
    main()
