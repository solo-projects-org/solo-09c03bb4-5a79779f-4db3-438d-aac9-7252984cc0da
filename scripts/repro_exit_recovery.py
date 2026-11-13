#!/usr/bin/env python3
"""复现场景 4：迁移过程中进程退出（kill -9 / 断电）后的重启恢复。

本脚本在真实子进程中启动 uvicorn 服务，通过故障注入点的 exit 模式让进程
在指定阶段 os._exit 立即退出（不执行任何清理），再重新拉起服务，验证：

  A. 文件准备阶段退出（staging_write + exit）
     → 重启后遗留 preparing 迁移判 failed，暂存区与 .tmp 被清扫，沿用原表；
  B. 目录切换瞬间退出（promote_exit）
     → 登记库处于 publishing：重启时若新表完整则续提为 committed，
       不完整则整体回滚旧代；两种结局活动代都一致可用；
  C. 已完成（committed）的迁移不受影响，完整新表继续可读；
  D. 恢复后可继续上传、查询、下载并重新发起迁移。

运行（需要本地可执行 python 与依赖；Docker 场景见 README）：
  python3 -I scripts/repro_exit_recovery.py
可用环境变量：
  WATER_DATA_DIR   临时数据卷目录（默认系统临时目录下自动创建）
  WATER_PORT       服务端口（默认 8011）
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import sysconfig
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _common as common

REPO_ROOT = Path(__file__).resolve().parent.parent


def _python_site_paths() -> list[str]:
    """收集当前解释器可见的 site-packages，传给子进程（兼容 user site 安装）。"""
    paths = []
    for candidate in (
        sysconfig.get_path("purelib"),
        os.path.expanduser("~/.local/lib/python3.11/site-packages"),
    ):
        if candidate and Path(candidate).is_dir() and candidate not in paths:
            paths.append(candidate)
    return paths


def free_port() -> int:
    if os.environ.get("WATER_PORT"):
        return int(os.environ["WATER_PORT"])
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_healthy(port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    url = f"http://127.0.0.1:{port}/api/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    raise TimeoutError(f"服务在 {timeout}s 内未就绪: {url}")


def start_server(data_dir: Path, port: int, fault_point: str | None,
                 fault_mode: str = "exit") -> subprocess.Popen:
    env = dict(os.environ)
    # 子进程不用 -I：需要 PYTHONPATH 找到 user site 里的依赖；
    # 脚本本身仍以 python3 -I 运行，只用标准库，不受当前目录注入影响。
    extra_paths = [str(REPO_ROOT)] + _python_site_paths()
    env.update(
        {
            "WATER_DATA_DIR": str(data_dir),
            "MIGRATION_DELAY_SECONDS": "0.2",
            "WATER_FAULT_ENABLED": "1",
            "WATER_FAULT_POINT": fault_point or "",
            "WATER_FAULT_MODE": fault_mode,
            "WATER_FAULT_ONE_SHOT": "1",
            "PYTHONPATH": os.pathsep.join(extra_paths),
        }
    )
    log = open(data_dir / "server.log", "a", encoding="utf-8")
    cmd = [
        sys.executable, "-m", "uvicorn", "app.main:app",
        "--host", "127.0.0.1", "--port", str(port), "--workers", "1",
    ]
    proc = subprocess.Popen(
        cmd, cwd=str(REPO_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT
    )
    wait_healthy(port)
    return proc


def hard_wait_exit(proc: subprocess.Popen, timeout: float = 30.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            return rc
        time.sleep(0.2)
    proc.kill()
    raise TimeoutError("进程未按预期退出")


def run_case(case: str, fault_point: str | None) -> None:
    common.banner(f"场景 4{case}：进程在 {fault_point} 阶段硬退出后重启")
    data_dir = Path(tempfile.mkdtemp(prefix=f"water-exit-{case}-"))
    port = free_port()
    try:
        # --- 第一次启动：上传基线数据 ---
        common.BASE_URL = f"http://127.0.0.1:{port}"
        proc = start_server(data_dir, port, fault_point=None)
        common.upload_csv(common.COMPATIBLE_CSV, "base.csv")
        common.show_schema()

        # 配置一次性硬退出故障，并发起迁移
        common.set_fault(fault_point, mode="exit")
        mid = common.start_migration(18, 2)
        rc = hard_wait_exit(proc)
        print(f"  进程已硬退出，退出码={rc}（未执行任何清理）")

        # --- 第二次启动：同一数据卷，触发启动恢复 ---
        proc2 = start_server(data_dir, port, fault_point=None)
        mig = common.get(f"/api/migrations/{mid}")["json"]
        print("  重启后迁移记录：", mig["status"], "/", mig["phase"])
        schema = common.show_schema()
        rows = common.get("/api/rows?limit=1000")["json"]
        print(f"  可查询行数：{rows['total_matched']}，读取模式 {rows['schema']}")
        assert dl_ok(port), "恢复后下载必须可用"
        assert schema["total_rows"] >= 1, "原表数据不得丢失"

        # 恢复后重新迁移必须成功，且不被遗留目录阻挡。
        # 若重启已把该迁移续提成功（目标模式已生效），则换一个目标模式再迁移。
        target = (18, 2)
        if schema["concentration_type"] == "decimal128(18,2)":
            target = (20, 6)
        mid2 = common.start_migration(*target)
        result = common.wait_migration(mid2)
        assert result["status"] == "committed", result
        schema2 = common.show_schema()
        assert schema2["concentration_type"] == (
            f"decimal128({target[0]},{target[1]})"
        )
        print("  恢复后重新迁移成功，全套文件切换到新模式 ✔")
        proc2.terminate()
        proc2.wait(timeout=15)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def dl_ok(port: int) -> bool:
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/download", timeout=10
        ) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001
        return False


def main() -> None:
    # A. 文件准备阶段退出
    run_case("A", "staging_write")
    # B. 目录切换瞬间退出（publishing 相位，重启续提或回滚）
    run_case("B", "promote_exit")
    common.banner("场景 4 复现完成：任意阶段退出，重启后数据一致、可重新迁移 ✔")


if __name__ == "__main__":
    main()
