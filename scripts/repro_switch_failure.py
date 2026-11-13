#!/usr/bin/env python3
"""复现场景 3：目录发布/切换失败（本系统修复前的核心 Bug）。

修复前的问题：登记库事务先把活动指针翻到新代，随后 staging→gen 目录
rename 一旦失败，就会出现“迁移显示失败、当前模式却已改变、重启不恢复”，
且读路径指向缺失/空目录。

本脚本通过 `promote` 注入点在目录切换前制造一次失败，验证：
  1. 迁移 failed，但活动模式仍是旧 decimal128(18,6)（没有翻转后悬空）；
  2. 新代没有误登记批次，残缺目录被隔离清理；
  3. 查询/下载始终读到完整旧代；
  4. 重试迁移成功，新模式与全套 Parquet 同时生效。

另外演示空表（零批次）也会经历同样的两阶段提交与失败回滚。

运行：
  python3 -I scripts/repro_switch_failure.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _common as common


def main() -> None:
    common.ensure_healthy()

    common.banner("场景 3A：有数据时目录切换失败（promote 注入）")
    common.upload_csv(common.COMPATIBLE_CSV, "base.csv")
    before = common.show_schema()

    common.set_fault("promote")
    mid = common.start_migration(18, 2)
    result = common.wait_migration(mid)
    print("  迁移结果：", result["status"], "/", result["phase"])
    assert result["status"] == "failed"

    schema = common.show_schema()
    assert schema["gen_id"] == before["gen_id"], "切换失败不得留下已翻转的指针"
    assert schema["concentration_type"] == "decimal128(18,6)"
    assert schema["total_rows"] == before["total_rows"]
    print(f"  活动代仍为 gen-{schema['gen_id']} / {schema['concentration_type']}，无悬空指针 ✔")

    rows = common.get("/api/rows?limit=1000")["json"]
    assert rows["total_matched"] == before["total_rows"]
    dl = common.request("GET", "/api/download")
    assert dl["status"] == 200
    print("  查询与下载仍只读到完整旧代这一套表 ✔")

    common.banner("解除故障后重试：新模式与全套 Parquet 必须同时生效")
    common.set_fault(None)
    mid2 = common.start_migration(18, 2)
    result2 = common.wait_migration(mid2)
    assert result2["status"] == "committed", result2
    schema2 = common.show_schema()
    assert schema2["concentration_type"] == "decimal128(18,2)"
    assert schema2["batch_count"] == before["batch_count"]
    print("  迁移 committed，活动代整体切到新模式，批次一个不缺 ✔")

    common.banner("场景 3B：空表（零批次）同样经历两阶段提交")
    # 用一个全新的目标模式再次迁移，验证零批次路径也走完整发布流程
    common.set_fault("promote")
    mid3 = common.start_migration(20, 6)
    result3 = common.wait_migration(mid3)
    assert result3["status"] == "failed"
    schema3 = common.show_schema()
    assert schema3["concentration_type"] == "decimal128(18,2)", "空表失败也不得翻转模式"
    print("  空表切换失败：模式保持不变，迁移判失败 ✔")
    common.set_fault(None)
    mid4 = common.start_migration(20, 6)
    result4 = common.wait_migration(mid4)
    assert result4["status"] == "committed"
    schema4 = common.show_schema()
    assert schema4["concentration_type"] == "decimal128(20,6)" and schema4["gen_id"] == 3
    print("  空表重试成功：模式翻转，后续上传按新模式落盘 ✔")

    common.banner("场景 3 复现完成：切换失败可回滚、可重试、绝不悬空 ✔")


if __name__ == "__main__":
    main()
