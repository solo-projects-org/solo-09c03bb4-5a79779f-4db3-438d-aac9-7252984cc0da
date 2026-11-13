"""故障注入与崩溃恢复测试：写盘错误、登记失败、切换时退出、清理失败、空表迁移。

每个用例使用独立临时数据目录；故障注入通过显式装配的 FaultInjector 触发，
exit 模式用抛 HardExitError 的替身代替 os._exit，避免杀死 pytest 进程。
"""
from __future__ import annotations

import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.faults import FaultInjector, HardExitError
from app.main import create_app
from app import storage


def client_post(c):
    return c.post(
        "/api/batches",
        files={"file": ("b.csv", "样本号,浓度\nX,1.000000\n".encode(), "text/csv")},
    )


@pytest.fixture()
def env(tmp_path):
    data_dir = tmp_path / "data"
    settings = Settings()
    settings.DATA_DIR = data_dir
    settings.MIGRATION_DELAY_SECONDS = 0.0
    # 测试用注入器：exit_func 替换为抛异常，绝不真正退出进程
    faults = FaultInjector(
        enabled=True,
        exit_func=lambda code: (_ for _ in ()).throw(
            HardExitError("simulated hard exit")
        ),
    )
    app = create_app(settings, faults=faults)
    with TestClient(app) as client:
        yield client, data_dir, settings, app, faults
    app.state.catalog.close()


def upload(client, text):
    resp = client.post(
        "/api/batches",
        files={"file": ("b.csv", text.encode("utf-8"), "text/csv")},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def start_migration(client, p, s):
    resp = client.post(f"/api/migrations?target_precision={p}&target_scale={s}")
    assert resp.status_code == 202, resp.text
    return resp.json()["migration_id"]


def wait_migration(client, mid, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/api/migrations/{mid}").json()
        if r["status"] != "running":
            return r
        time.sleep(0.02)
    raise AssertionError("迁移未在时限内结束")


def set_fault(faults, point, mode="raise"):
    faults.reset()
    faults.configure(enabled=True, point=point, mode=mode, one_shot=True)


# ------------------------------------------------------------ 迁移写盘失败

def test_staging_write_failure_keeps_old_schema_and_batches(env):
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    set_fault(faults, "staging_write")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    # 当前模式未被翻转
    schema = c.get("/api/schema").json()
    assert schema["concentration_type"] == "decimal128(18,6)"
    assert schema["gen_id"] == 1
    assert schema["total_rows"] == 2
    # 暂存区已清理，没有残缺新代
    assert not (data_dir / "parquet" / f"staging-{mid}").exists()
    assert not (data_dir / "parquet" / "gen-2").exists()
    # 故障解除后上传/查询/下载/重新迁移全部可用
    faults.reset()
    upload(c, "样本号,浓度\nC,3.500000\n")
    assert c.get("/api/schema").json()["total_rows"] == 3
    mid2 = start_migration(c, 18, 2)
    assert wait_migration(c, mid2)["status"] == "committed"
    assert c.get("/api/schema").json()["concentration_type"] == "decimal128(18,2)"
    z = c.get("/api/download")
    assert z.status_code == 200


def test_retry_after_failure_uses_fresh_staging_not_remnants(env):
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\n")
    set_fault(faults, "staging_write")
    mid1 = start_migration(c, 18, 2)
    wait_migration(c, mid1)
    faults.reset()
    # 无残留暂存目录阻挡
    leftovers = [
        p.name
        for p in (data_dir / "parquet").iterdir()
        if p.name.startswith("staging-")
    ]
    assert leftovers == []
    mid2 = start_migration(c, 18, 2)
    assert mid2 != mid1
    assert wait_migration(c, mid2)["status"] == "committed"
    gen2 = data_dir / "parquet" / "gen-2"
    files = list(gen2.glob("*.parquet"))
    assert len(files) == 1
    t = pq.read_table(files[0])
    assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"


# ------------------------------------------------------------ 登记失败

def test_db_commit_failure_rolls_back_entirely(env):
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    set_fault(faults, "db_commit")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    schema = c.get("/api/schema").json()
    assert schema["gen_id"] == 1
    assert schema["concentration_type"] == "decimal128(18,6)"
    assert schema["total_rows"] == 2
    # 新代没有任何登记批次，暂存区被清理
    cat = app.state.catalog
    assert cat.list_batches(2) == []
    assert not (data_dir / "parquet" / "gen-2").exists()
    # 重试成功
    faults.reset()
    mid2 = start_migration(c, 18, 2)
    assert wait_migration(c, mid2)["status"] == "committed"


# ------------------------------------------------------------ 切换失败（发布后）

def test_promote_failure_rolls_back_pointer_and_batches(env):
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    set_fault(faults, "promote")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    # 关键：模式不能停在已翻转但目录缺失的状态
    schema = c.get("/api/schema").json()
    assert schema["gen_id"] == 1
    assert schema["concentration_type"] == "decimal128(18,6)"
    assert schema["total_rows"] == 2
    cat = app.state.catalog
    assert cat.list_batches(2) == []
    # 查询/下载仍读完整旧代
    rows = c.get("/api/rows").json()
    assert rows["total_matched"] == 2
    assert c.get("/api/download").status_code == 200
    # 残缺目录被隔离清理，重试不被阻挡
    assert not (data_dir / "parquet" / "gen-2").exists()
    faults.reset()
    mid2 = start_migration(c, 18, 2)
    assert wait_migration(c, mid2)["status"] == "committed"
    assert c.get("/api/schema").json()["gen_id"] == 2


# ------------------------------------------------------------ 切换时退出 → 重启恢复

def test_hard_exit_during_preparing_restart_marks_failed_keeps_old(env):
    """文件准备阶段被杀：重启后遗留 preparing 迁移判失败，暂存区清理，原表可用。"""
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")

    # 直接构造“迁移进行中进程被杀”的现场：running/preparing + 半成品暂存区
    cat = app.state.catalog
    with cat.lock:
        mid = cat.start_migration(18, 2, staging_gen_id=2)
    staging = storage.staging_dir(settings.parquet_root, mid)
    # 写一个残缺临时文件，验证重启清扫
    (staging / "half.parquet.tmp").write_bytes(b"not-a-parquet")
    assert cat.get_migration(mid)["phase"] == "preparing"

    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        mig = c2.get(f"/api/migrations/{mid}").json()
        assert mig["status"] == "failed"
        assert mig["phase"] == "failed"
        schema = c2.get("/api/schema").json()
        assert schema["gen_id"] == 1
        assert schema["concentration_type"] == "decimal128(18,6)"
        assert schema["total_rows"] == 2
        assert not staging.exists()
        # 恢复后可重新迁移
        mid2 = start_migration(c2, 18, 2)
        assert wait_migration(c2, mid2)["status"] == "committed"
    restarted.state.catalog.close()


def test_hard_exit_during_promotion_restart_rolls_back_to_old(env):
    """切换瞬间退出：publishing 相位重启后新表不完整 → 回滚旧表。

    真实 os._exit 会绕过线程异常处理，直接留下“库已 publishing、目录未提升”
    的现场；这里直接在登记库/磁盘上构造该现场来验证重启对账。
    """
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")

    # 1) 正常跑一遍到暂存区就绪，再手工把现场改成“切换瞬间被杀”
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "committed"
    cat = app.state.catalog
    gen2_files = storage.parquet_files(data_dir / "parquet" / "gen-2")
    assert len(gen2_files) == 1  # 一个 CSV = 一个批次文件（内含 2 行）

    # 2) 回退登记库到 publishing：指针翻到 gen-2，迁移仍 running
    with cat.lock:
        cat._conn.execute("BEGIN IMMEDIATE")
        cat.set_active(2, 18, 2)
        cat._conn.execute(
            "UPDATE migrations SET status='running', phase='publishing' WHERE id=?",
            (mid,),
        )
        cat._conn.execute("COMMIT")
    # 3) 物理上把 gen-2 移回暂存区，再删掉一个文件，模拟切换时只写了一半
    staging = storage.staging_dir(settings.parquet_root, mid)
    (data_dir / "parquet" / "gen-2").replace(staging)
    only = storage.parquet_files(staging)[0]
    only.unlink()
    assert cat.get_active()["gen_id"] == 2
    assert cat.get_migration(mid)["phase"] == "publishing"

    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        mig2 = c2.get(f"/api/migrations/{mid}").json()
        assert mig2["status"] == "failed"
        schema = c2.get("/api/schema").json()
        assert schema["gen_id"] == 1
        assert schema["concentration_type"] == "decimal128(18,6)"
        assert schema["total_rows"] == 2
        assert restarted.state.catalog.list_batches(2) == []
        rows = {r["样本号"] for r in c2.get("/api/rows").json()["rows"]}
        assert rows == {"A", "B"}
        # 重新迁移成功，且不被残留 gen-2/staging 阻挡
        mid2 = start_migration(c2, 18, 2)
        assert wait_migration(c2, mid2)["status"] == "committed"
        assert c2.get("/api/schema").json()["gen_id"] == 2
    restarted.state.catalog.close()


def test_hard_exit_during_promotion_restart_resumes_when_staging_complete(env):
    """切换瞬间退出但暂存区完整 → 重启续提成功，新模式与全套文件一起生效。"""
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    assert wait_migration(c, mid)["status"] == "committed"
    cat = app.state.catalog
    with cat.lock:
        cat._conn.execute("BEGIN IMMEDIATE")
        cat.set_active(2, 18, 2)
        cat._conn.execute(
            "UPDATE migrations SET status='running', phase='publishing' WHERE id=?",
            (mid,),
        )
        cat._conn.execute("COMMIT")
    staging = storage.staging_dir(settings.parquet_root, mid)
    (data_dir / "parquet" / "gen-2").replace(staging)

    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        mig2 = c2.get(f"/api/migrations/{mid}").json()
        assert mig2["status"] == "committed"
        schema = c2.get("/api/schema").json()
        assert schema["gen_id"] == 2
        assert schema["concentration_type"] == "decimal128(18,2)"
        assert schema["total_rows"] == 2
        # 全套文件均为新模式
        for f in storage.parquet_files(data_dir / "parquet" / "gen-2"):
            t = pq.read_table(f)
            assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
        assert not staging.exists()
    restarted.state.catalog.close()


def test_hard_exit_after_promotion_restart_confirms_commit(env):
    """极端情况：目录已提升、committed 标记前退出 → 重启校验完整后续提成功。"""
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\nB,2.000000\n")
    mid = start_migration(c, 18, 2)
    wait_migration(c, mid)
    # 手工构造“publishing 但物理新表已完整”的现场
    cat = app.state.catalog
    with cat.lock:
        cat._conn.execute(
            "UPDATE migrations SET status='running', phase='publishing' WHERE id=?",
            (mid,),
        )
    # 物理 gen-2 已存在且完整（上一步成功迁移产生）
    assert (data_dir / "parquet" / "gen-2").is_dir()

    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        mig2 = c2.get(f"/api/migrations/{mid}").json()
        assert mig2["status"] == "committed"
        schema = c2.get("/api/schema").json()
        assert schema["gen_id"] == 2
        assert schema["concentration_type"] == "decimal128(18,2)"
        assert schema["total_rows"] == 2
    restarted.state.catalog.close()


# ------------------------------------------------------------ 清理再次失败

def test_cleanup_failure_does_not_corrupt_and_retry_succeeds(env, monkeypatch):
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.230000\n")

    # 让暂存区清理两次都失败（rmtree 与隔离 move 均失败）
    real_rmtree = storage.shutil.rmtree
    real_move = storage.shutil.move

    def boom_rmtree(path, *a, **k):
        p = Path(path)
        if "staging" in p.name:
            raise OSError("simulated rmtree failure")
        return real_rmtree(path, *a, **k)

    def boom_move(src, dst):
        if "staging" in Path(src).name:
            raise OSError("simulated move failure")
        return real_move(src, dst)

    monkeypatch.setattr(storage.shutil, "rmtree", boom_rmtree)
    monkeypatch.setattr(storage.shutil, "move", boom_move)
    set_fault(faults, "staging_write")
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    # 有效数据完好
    schema = c.get("/api/schema").json()
    assert schema["gen_id"] == 1 and schema["total_rows"] == 1
    monkeypatch.undo()

    # 重启：遗留暂存区被对账清扫，重试不被阻挡、不复用残缺文件
    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        assert not (data_dir / "parquet" / f"staging-{mid}").exists()
        mid2 = start_migration(c2, 18, 2)
        assert wait_migration(c2, mid2)["status"] == "committed"
        gen2 = data_dir / "parquet" / "gen-2"
        for f in gen2.glob("*.parquet"):
            t = pq.read_table(f)
            assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
    restarted.state.catalog.close()


# ------------------------------------------------------------ 上传故障

def test_upload_write_failure_leaves_nothing(env):
    c, data_dir, settings, app, faults = env
    set_fault(faults, "upload_write")
    resp = client_post(c)
    assert resp.status_code == 400
    # 无登记、无正式文件、最多只有被清扫的 tmp
    cat = app.state.catalog
    assert cat.list_batches(1) == []
    assert list((data_dir / "parquet" / "gen-1").glob("*.parquet")) == []
    faults.reset()
    upload(c, "样本号,浓度\nA,1.000000\n")
    assert c.get("/api/schema").json()["total_rows"] == 1


def test_upload_register_failure_rolls_back_file(env, monkeypatch):
    c, data_dir, settings, app, faults = env
    set_fault(faults, "upload_register")
    resp = client_post(c)
    assert resp.status_code == 400
    cat = app.state.catalog
    assert cat.list_batches(1) == []
    assert list((data_dir / "parquet" / "gen-1").glob("*.parquet")) == []
    faults.reset()
    upload(c, "样本号,浓度\nA,1.000000\n")
    assert c.get("/api/schema").json()["total_rows"] == 1


def test_upload_cleanup_failure_quarantines_no_corruption(env, monkeypatch):
    c, data_dir, settings, app, faults = env
    real_unlink = Path.unlink

    def boom_unlink(self, *a, **k):
        if self.suffix == ".parquet":
            raise OSError("simulated unlink failure")
        return real_unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", boom_unlink)
    set_fault(faults, "upload_register")
    resp = client_post(c)
    assert resp.status_code == 400
    monkeypatch.undo()
    # 登记里没有该批；孤儿文件在重启对账时被隔离，不影响活动代
    app.state.catalog.close()
    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        schema = c2.get("/api/schema").json()
        assert schema["batch_count"] == 0
        registered = {
            b["filename"] for b in restarted.state.catalog.list_batches(1)
        }
        on_disk = {p.name for p in (data_dir / "parquet" / "gen-1").glob("*.parquet")}
        assert on_disk <= registered
        upload(c2, "样本号,浓度\nA,1.000000\n")
        assert c2.get("/api/schema").json()["total_rows"] == 1
    restarted.state.catalog.close()


# ------------------------------------------------------------ 空表也能迁移

def test_migration_with_zero_batches_succeeds_and_switches_schema(env):
    c, data_dir, settings, app, faults = env
    # 没有任何批次时发起迁移（空表场景）
    mid = start_migration(c, 18, 2)
    result = wait_migration(c, mid)
    assert result["status"] == "committed"
    schema = c.get("/api/schema").json()
    assert schema["gen_id"] == 2
    assert schema["concentration_type"] == "decimal128(18,2)"
    assert schema["batch_count"] == 0
    # 切换后上传按新模式落新代
    info = upload(c, "样本号,浓度\nA,5.000000\n")
    assert info["gen_id"] == 2 and (info["precision"], info["scale"]) == (18, 2)
    t = pq.read_table(data_dir / "parquet" / "gen-2" / f"{info['batch_id']}.parquet")
    assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
    # 下载包结构自洽（manifest + 0 或 1 个 parquet）
    z = c.get("/api/download")
    assert z.status_code == 200


def test_empty_table_promote_failure_then_retry(env):
    c, data_dir, settings, app, faults = env
    set_fault(faults, "promote")
    mid = start_migration(c, 20, 6)
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    assert c.get("/api/schema").json()["gen_id"] == 1
    faults.reset()
    mid2 = start_migration(c, 20, 6)
    assert wait_migration(c, mid2)["status"] == "committed"
    assert c.get("/api/schema").json()["gen_id"] == 2


# ------------------------------------------------------------ 管理接口

def test_fault_admin_endpoints(env):
    c, *_ = env
    r = c.post(
        "/api/admin/faults",
        json={"point": "db_commit", "mode": "raise", "one_shot": True},
    )
    assert r.status_code == 200
    assert r.json()["point"] == "db_commit"
    r = c.post("/api/admin/faults", json={"point": "nope"})
    assert r.status_code == 422
    r = c.delete("/api/admin/faults")
    assert r.status_code == 200
    assert r.json()["point"] is None


def test_fault_endpoints_disabled_when_env_off(tmp_path):
    settings = Settings()
    settings.DATA_DIR = tmp_path / "data"
    settings.FAULT_ENABLED = False
    faults = FaultInjector(enabled=True)
    app = create_app(settings, faults=faults)
    with TestClient(app) as c:
        r = c.post("/api/admin/faults", json={"point": "promote"})
        assert r.status_code == 403
        assert c.get("/api/admin/faults").json()["enabled"] is False
        # 即便注入器里残留配置，迁移也不受影响
        faults.point = "promote"
        upload(c, "样本号,浓度\nA,1.000000\n")
        mid = start_migration(c, 18, 2)
        assert wait_migration(c, mid)["status"] == "committed"
    app.state.catalog.close()


# ------------------------------------------------------------ 读路径保护与孤儿对账

def test_read_path_returns_503_when_active_file_missing(env):
    """活动代登记的文件物理缺失时，查询/下载拒绝返回残缺的一套。"""
    c, data_dir, settings, app, faults = env
    info = upload(c, "样本号,浓度\nA,1.000000\nB,2.000000\n")
    # 人为删掉活动代的正式文件
    target = data_dir / "parquet" / "gen-1" / f"{info['batch_id']}.parquet"
    target.unlink()
    assert c.get("/api/rows").status_code == 503
    assert c.get("/api/download").status_code == 503
    assert c.get("/api/schema").status_code == 503
    # 健康检查显式报告不可用，而不是假装正常
    assert c.get("/api/health").status_code == 503


def test_restart_reconciles_orphan_dirs_and_files(env):
    """无主暂存区/代目录与未登记孤儿文件在重启时被清扫，不阻挡后续迁移。"""
    c, data_dir, settings, app, faults = env
    upload(c, "样本号,浓度\nA,1.000000\n")
    app.state.catalog.close()

    root = settings.parquet_root
    # 伪造无主暂存区、无主代目录、孤儿 parquet 与残留 tmp
    (root / "staging-999").mkdir(parents=True)
    (root / "staging-999" / "x.parquet.tmp").write_bytes(b"junk")
    (root / "gen-77").mkdir(parents=True)
    (root / "gen-77" / "ghost.parquet").write_bytes(b"junk")
    (root / "gen-1" / "orphan.parquet").write_bytes(b"junk")
    (root / "gen-1" / "leftover.parquet.tmp").write_bytes(b"junk")

    faults2 = FaultInjector(enabled=False)
    restarted = create_app(settings, faults=faults2)
    with TestClient(restarted) as c2:
        assert not (root / "staging-999").exists()
        assert not (root / "gen-77").exists()
        gen1 = root / "gen-1"
        names = {p.name for p in gen1.iterdir()}
        assert "orphan.parquet" not in names
        assert not any(n.endswith(".tmp") for n in names)
        # 有效数据仍在，且可以正常迁移
        mid = start_migration(c2, 18, 2)
        assert wait_migration(c2, mid)["status"] == "committed"
    restarted.state.catalog.close()


# ------------------------------------------------------------ 迁移中上传 + 故障

def test_failed_migration_keeps_batches_received_during_window(env):
    c, data_dir, settings, app, faults = env
    settings.MIGRATION_DELAY_SECONDS = 0.3
    c.app.state.service.migration_delay_seconds = 0.3
    upload(c, "样本号,浓度\nA,1.230000\n")
    set_fault(faults, "staging_write")
    mid = start_migration(c, 18, 2)
    time.sleep(0.1)
    # 迁移窗口内继续接收新批
    upload(c, "样本号,浓度\nMID,2.000000\n")
    result = wait_migration(c, mid)
    assert result["status"] == "failed"
    schema = c.get("/api/schema").json()
    assert schema["gen_id"] == 1 and schema["total_rows"] == 2
    rows = {r["样本号"] for r in c.get("/api/rows").json()["rows"]}
    assert rows == {"A", "MID"}
    # 重试成功后两批一起进新代
    faults.reset()
    mid2 = start_migration(c, 18, 2)
    assert wait_migration(c, mid2)["status"] == "committed"
    gen2 = data_dir / "parquet" / "gen-2"
    all_samples = set()
    for f in gen2.glob("*.parquet"):
        t = pq.read_table(f)
        assert str(t.schema.field("浓度").type) == "decimal128(18, 2)"
        all_samples.update(t.column("样本号").to_pylist())
    assert all_samples == {"A", "MID"}
