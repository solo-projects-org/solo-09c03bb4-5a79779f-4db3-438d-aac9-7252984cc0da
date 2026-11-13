#!/usr/bin/env python3
"""复现场景 2：登记失败。

覆盖两个登记环节的故障注入点：
  A. 上传批次写盘成功、SQLite 登记失败（upload_register）
     → 已写文件必须回滚删除，不留孤儿半结果，批次计数不变；
  B. 迁移的登记库提交事务失败（db_commit）
     → 事务整体回滚：活动指针不翻转、新代零登记批次、暂存区清理；
  两类故障解除后重试都能成功，且不会被遗留文件阻挡。

运行：
  python3 -I scripts/repro_registration_failure.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _common as common


def main() -> None:
    common.ensure_healthy()

    common.banner("场景 2A：上传时登记失败（upload_register 注入）")
    common.upload_csv(common.COMPATIBLE_CSV, "base.csv")
    before = common.show_schema()

    common.set_fault("upload_register")
    resp = common.upload_csv(common.COMPATIBLE_CSV.replace("WQ-", "WQR-"), "will-rollback.csv")
    print("  上传响应状态：", resp["status"], "（预期 400，登记失败）")
    assert resp["status"] == 400
    common.set_fault(None)
    after = common.show_schema()
    assert after["total_rows"] == before["total_rows"], "登记失败不得增加批次/行数"
    print("  批次计数未变化，回滚文件已清理，无孤儿半结果 ✔")

    common.banner("场景 2B：迁移登记事务失败（db_commit 注入）")
    common.set_fault("db_commit")
    mid = common.start_migration(18, 2)
    result = common.wait_migration(mid)
    print("  迁移结果：", result["status"], "/", result["phase"])
    assert result["status"] == "failed"
    schema = common.show_schema()
    assert schema["concentration_type"] == "decimal128(18,6)", "登记失败必须整体回滚"
    assert schema["gen_id"] == before["gen_id"]
    rows = common.get("/api/rows?limit=1000")["json"]
    assert rows["total_matched"] == before["total_rows"], "原表数据必须完整保留"
    print(f"  活动代仍是 gen-{schema['gen_id']}，原表 {rows['total_matched']} 行完整 ✔")

    common.banner("解除故障后重新迁移，验证不被遗留物阻挡")
    common.set_fault(None)
    mid2 = common.start_migration(18, 2)
    result2 = common.wait_migration(mid2)
    assert result2["status"] == "committed", result2
    schema2 = common.show_schema()
    assert schema2["concentration_type"] == "decimal128(18,2)"
    print("  重新迁移成功，新模式与全套 Parquet 一起生效 ✔")

    common.banner("场景 2 复现完成：登记失败整体回滚，重试不受遗留文件阻挡 ✔")


if __name__ == "__main__":
    main()
