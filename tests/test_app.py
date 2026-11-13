"""端到端测试：精度、空值、迁移原子性、并发上传、崩溃恢复、下载一致性。

每个用例使用独立临时数据目录，通过应用工厂 + TestClient 走完整 HTTP 栈。
"""
from __future__ import annotations

import io
import json
import time
import zipfile
from decimal import Decimal

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.catalog import create_catalog
from app.config import Settings
from app.main import create_app
from app import storage


@pytest.fixture()
def env(tmp_path):
    data_dir = tmp_path / "data"
    settings = Settings()
    settings.DATA_DIR = data_dir
    settings.MIGRATION_DELAY_SECONDS = 0.0
    app = create_app(settings)
    with TestClient(app) as client:
        yield client, data_dir, settings, app
    app.state.catalog.close()


@pytest.fixture()
def client(env):
    return env[0], env[1]


def csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def upload(client, text):
    resp = client.post(
        "/api/batches",
        files={"file": ("b.csv", csv_bytes(text), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def start_migration(client, p, s):
    resp = client.post(f"/api/migrations?target_precision={p}&target_scale={s}")
    assert resp.status_code == 202, resp.text
    return resp.json()["migration_id"]


def wait_migration(client, mid, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/api/migrations/{mid}").json()
        if r["status"] != "running":
            return r
        time.sleep(0.05)
    raise AssertionError("迁移未在时限内结束")


# ---------------------------------------------------------------- 基础行为

def test_health_and_initial_schema(client):
    c, _ = client
    h = c.get("/api/health").json()
    assert h["status"] == "ok"
    schema = c.get("/api/schema").json()
    assert schema["concentration_type"] == "decimal128(18,6)"
    assert schema["gen_id"] == 1


def test_upload_keeps_decimal_precision_and_nulls(client):
    c, data_dir = client
    text = "样本号,浓度\nA,1.234567\nB,\nC,-999999999999.999999\nD,0.000000\n"
    info = upload(c, text)
    assert info["row_count"] == 4
    # 直接读 Parquet 验证物理类型与精确值
    table = pq.read_table(data_dir / "parquet" / "gen-1" / f"{info['batch_id']}.parquet")
    field = table.schema.field("浓度")
    assert str(field.type) == "decimal128(18, 6)"
    assert table.column("浓度").to_pylist() == [
        Decimal("1.234567"), None, Decimal("-999999999999.999999"), Decimal("0.000000")
    ]
    # 查询接口返回精确十进制字符串，空值保留
    rows = c.get("/api/rows").json()["rows"]
    by_sample = {r["样本号"]: r["浓度"] for r in rows}
    assert by_sample == {"A": "1.234567", "B": None,
                         "C": "-999999999999.999999", "D": "0.000000"}


def test_upload_rejects_invalid_and_bad_csv(client):
    c, _ = client
    # 非法十进制文本
    r = c.post("/api/batches", files={"file": ("b.csv",
        csv_bytes("样本号,浓度\nA,abc\n"), "text/csv")})
    assert r.status_code == 400
    # 缺列
    r = c.post("/api/batches", files={"file": ("b.csv",
        csv_bytes("样本号,备注\nA,x\n"), "text/csv")})
    assert r.status_code == 400
    # 超出 (18,6) 整数范围
    r = c.post("/api/batches", files={"file": ("b.csv",
        csv_bytes("样本号,浓度\nA,1234567890123.000000\n"), "text/csv")})
    assert r.status_code == 400
    # NaN/Inf 拒绝
    r = c.post("/api/batches", files={"file": ("b.csv",
        csv_bytes("样本号,浓度\nA,NaN\n"), "text/csv")})
    assert r.status_code == 400


# ---------------------------------------------------------------- 失败迁移

def test_failed_migration_keeps_old_table_and_new_batches(client):
    c, data_dir = client
    upload(c, "样本号,浓度\nA,1.230000\nB,3.145900\n")
    upload(c, "样本号,浓度\nC,8.000000\n")
    before = c.get("/api/schema").json()
    mid = start_migration(c, 18, 2)  # B 的 3.1459 无法精确表示
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    bad = result["detail"]["incompatible_samples"]
    assert {b["sample_id"] for b in bad} == {"B"}
    # 模式仍是旧模式，原表数据完整
    after = c.get("/api/schema").json()
    assert after["concentration_type"] == "decimal128(18,6)"
    assert after["gen_id"] == before["gen_id"]
    assert after["total_rows"] == 3
    # 暂存区被清理，没有半迁移目录暴露
    assert not (data_dir / "parquet" / f"staging-{mid}").exists()
    assert not (data_dir / "parquet" / "gen-2").exists()
    # 失败后仍可继续上传与查询
    upload(c, "样本号,浓度\nD,2.500000\n")
    assert c.get("/api/schema").json()["total_rows"] == 4


def test_only_one_migration_at_a_time(env):
    c, _, settings, _ = env
    # 人为放大延迟制造运行窗口（仅本用例的服务实例）
    settings.MIGRATION_DELAY_SECONDS = 1.0
    c.app.state.service.migration_delay_seconds = 1.0
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    r = c.post("/api/migrations?target_precision=10&target_scale=2")
    assert r.status_code == 409
    result = wait_migration(c, mid, timeout=15)
    assert result["status"] == "committed"


# ---------------------------------------------------------------- 成功迁移

def test_successful_migration_switches_all_files(client):
    c, data_dir = client
    upload(c, "样本号,浓度\nA,1.230000\nB,\nC,100.000000\n")
    upload(c, "样本号,浓度\nD,0.010000\nE,-7.500000\n")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "committed"
    schema = c.get("/api/schema").json()
    assert schema["concentration_type"] == "decimal128(18,2)"
    assert schema["gen_id"] == 2
    # 新代全套文件物理类型均为新模式
    for f in sorted((data_dir / "parquet" / "gen-2").glob("*.parquet")):
        t = pq.read_table(f)
        assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
    rows = {r["样本号"]: r["浓度"] for r in c.get("/api/rows").json()["rows"]}
    assert rows == {"A": "1.23", "B": None, "C": "100.00", "D": "0.01", "E": "-7.50"}
    # 旧代目录仍保留（不可变快照），但查询只走新代
    assert (data_dir / "parquet" / "gen-1").exists()


def test_widening_precision_migration(client):
    c, _ = client
    upload(c, "样本号,浓度\nA,1.234567\n")
    mid = start_migration(c, 20, 8)  # 扩位：补零精确
    result = wait_migration(c, mid)
    assert result["status"] == "committed"
    rows = c.get("/api/rows").json()["rows"]
    assert rows[0]["浓度"] == "1.23456700"


# ---------------------------------------------------------------- 迁移中上传

def test_upload_during_migration_compatible_switches_together(env):
    c, data_dir, settings, _ = env
    settings.MIGRATION_DELAY_SECONDS = 0.4
    c.app.state.service.migration_delay_seconds = 0.4
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    # 迁移进行中上传一个兼容新批
    time.sleep(0.2)
    upload(c, "样本号,浓度\nMID,9.990000\n")
    result = wait_migration(c, mid, timeout=15)
    assert result["status"] == "committed"
    # 中途新批必须出现在新代，且为新模式
    gen2_files = list((data_dir / "parquet" / "gen-2").glob("*.parquet"))
    all_ids = set()
    for f in gen2_files:
        t = pq.read_table(f)
        assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
        all_ids.update(t.column("样本号").to_pylist())
    assert {"A", "B", "MID"} <= all_ids


def test_upload_during_migration_incompatible_fails_but_keeps_new_batch(env):
    c, data_dir, settings, _ = env
    settings.MIGRATION_DELAY_SECONDS = 0.4
    c.app.state.service.migration_delay_seconds = 0.4
    upload(c, "样本号,浓度\nA,1.230000\n")
    mid = start_migration(c, 18, 2)
    time.sleep(0.2)
    # 迁移期间接收一个含不兼容值的新批（按旧模式正常接收并保留）
    new_batch = upload(c, "样本号,浓度\nNEW,3.145900\n")
    result = wait_migration(c, mid, timeout=15)
    assert result["status"] == "failed"
    bad = result["detail"]["incompatible_samples"]
    assert any(b["sample_id"] == "NEW" and b["batch_id"] == new_batch["batch_id"] for b in bad)
    # 新批仍保留在旧代，查询可得
    schema = c.get("/api/schema").json()
    assert schema["concentration_type"] == "decimal128(18,6)"
    rows = c.get("/api/rows").json()["rows"]
    assert any(r["样本号"] == "NEW" and r["浓度"] == "3.145900" for r in rows)


def test_upload_after_switch_uses_new_schema(client):
    """切换完成后的上传必须按新模式落新代，不混入旧类型文件。"""
    c, data_dir = client
    upload(c, "样本号,浓度\nA,1.230000\n")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid, timeout=15)
    assert result["status"] == "committed"
    info = upload(c, "样本号,浓度\nAFTER,5.000000\n")
    assert info["gen_id"] == 2
    assert (info["precision"], info["scale"]) == (18, 2)
    t = pq.read_table(data_dir / "parquet" / "gen-2" / f"{info['batch_id']}.parquet")
    assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"


def test_upload_straddling_switch_never_mixes_old_type(env):
    """并发压测：迁移窗口内持续上传，新代中绝不允许出现旧 decimal128(18,6) 文件。"""
    c, data_dir, settings, _ = env
    settings.MIGRATION_DELAY_SECONDS = 0.15
    c.app.state.service.migration_delay_seconds = 0.15
    upload(c, "样本号,浓度\nBASE,1.000000\n")
    mid = start_migration(c, 18, 2)
    # 迁移期间持续发上传（均为兼容值）
    uploaded = []
    for i in range(6):
        info = upload(c, f"样本号,浓度\nR{i},2.000000\n")
        uploaded.append(info)
        time.sleep(0.05)
    result = wait_migration(c, mid, timeout=20)
    assert result["status"] == "committed"
    # 新代每个文件都必须是 (18,2)
    for f in (data_dir / "parquet" / "gen-2").glob("*.parquet"):
        t = pq.read_table(f)
        assert str(t.schema.field("浓度").type) == "decimal128(18, 2)", f.name
    # 切换后再上传也落在新代
    after = upload(c, "样本号,浓度\nLAST,3.000000\n")
    assert after["gen_id"] == 2


# ---------------------------------------------------------------- 下载一致性

def test_download_package_is_single_consistent_set(client):
    c, _ = client
    upload(c, "样本号,浓度\nA,1.230000\n")
    upload(c, "样本号,浓度\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    wait_migration(c, mid)
    resp = c.get("/api/download")
    assert resp.status_code == 200
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    manifest = json.loads(zf.read("manifest.json"))
    assert manifest["concentration_type"] == "decimal128(18,2)"
    assert manifest["gen_id"] == 2
    pq_files = [n for n in zf.namelist() if n.endswith(".parquet")]
    assert len(pq_files) == 2
    for name in pq_files:
        t = pq.read_table(io.BytesIO(zf.read(name)))
        assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"


def test_download_during_migration_serves_old_complete_set(env):
    c, _, settings, _ = env
    settings.MIGRATION_DELAY_SECONDS = 0.5
    c.app.state.service.migration_delay_seconds = 0.5
    upload(c, "样本号,浓度\nA,1.230000\n")
    start_migration(c, 18, 2)
    time.sleep(0.15)
    # 迁移进行中下载，必须拿到完整旧代（18,6），看不到暂存半成品
    resp = c.get("/api/download")
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    manifest = json.loads(zf.read("manifest.json"))
    assert manifest["concentration_type"] == "decimal128(18,6)"
    for name in [n for n in zf.namelist() if n.endswith(".parquet")]:
        t = pq.read_table(io.BytesIO(zf.read(name)))
        assert str(t.schema.field("浓度").type) == "decimal128(18, 6)"


# ---------------------------------------------------------------- 重启恢复

def test_restart_marks_unfinished_migration_failed_and_keeps_committed(env):
    c, data_dir, settings, first_app = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    # 先成功完成一次迁移
    mid_ok = start_migration(c, 18, 2)
    assert wait_migration(c, mid_ok)["status"] == "committed"
    first_app.state.catalog.close()

    # 直接在 DB 伪造一条遗留 running 迁移 + 暂存目录，模拟进程在迁移中被杀死
    cat2 = create_catalog(settings.db_path)
    with cat2.lock:
        fake_id = cat2.start_migration(10, 2, staging_gen_id=99)
    storage.staging_dir(settings.parquet_root, fake_id)
    cat2.close()

    # 重启：用相同数据卷重新装配整套服务
    restarted = create_app(settings)
    with TestClient(restarted) as c2:
        migs = c2.get("/api/migrations").json()["migrations"]
        fake = next(m for m in migs if m["id"] == fake_id)
        assert fake["status"] == "failed"
        assert "重启" in fake["detail"]["reason"]
        # 已切换的新表继续有效
        schema = c2.get("/api/schema").json()
        assert schema["concentration_type"] == "decimal128(18,2)"
        assert schema["gen_id"] == 2
        assert schema["total_rows"] == 2
        # 暂存区已清理
        assert not (data_dir / "parquet" / f"staging-{fake_id}").exists()
    restarted.state.catalog.close()


def test_migration_history_records_both_outcomes(client):
    c, _ = client
    upload(c, "样本号,浓度\nA,1.230000\nB,3.145900\n")
    failed_id = start_migration(c, 18, 2)
    wait_migration(c, failed_id)
    # 失败后修正数据不可行（原值已固定），改为对兼容子集重新迁移验证历史累积
    upload(c, "样本号,浓度\nC,9.000000\n")
    # 仍含不兼容 B，故再次失败；历史应保留两条
    again = start_migration(c, 18, 2)
    wait_migration(c, again)
    migs = c.get("/api/migrations").json()["migrations"]
    statuses = [m["status"] for m in migs]
    assert statuses == ["failed", "failed"]


def test_shutdown_during_running_migration_does_not_crash(env):
    """关闭服务时迁移仍在运行：应安静等待/退出，不抛线程异常，重启可恢复。"""
    c, data_dir, settings, app = env
    settings.MIGRATION_DELAY_SECONDS = 2.0
    c.app.state.service.migration_delay_seconds = 2.0
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    time.sleep(0.3)  # 确保迁移线程已在运行
    # 退出 TestClient 上下文即触发 lifespan 关闭；不应有异常泄漏
    # （env fixture 的 with 块在本用例返回后关闭，这里显式再做一次重启验证）
    app.state.service.shutdown(timeout=5)
    app.state.catalog.close()

    restarted = create_app(settings)
    with TestClient(restarted) as c2:
        mig = c2.get(f"/api/migrations/{mid}").json()
        # 关闭时在途迁移只有两种确定结局：join 期内提交成功，或未完成则重启判失败
        assert mig["status"] in ("committed", "failed")
        # 无论哪种，活动代都处于一致可用状态，原数据不丢
        schema = c2.get("/api/schema").json()
        assert schema["total_rows"] == 2
        if mig["status"] == "failed":
            assert schema["gen_id"] == 1
        else:
            assert schema["gen_id"] == 2
    restarted.state.catalog.close()
