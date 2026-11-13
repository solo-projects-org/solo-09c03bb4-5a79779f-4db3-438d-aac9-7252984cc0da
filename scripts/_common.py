#!/usr/bin/env python3
"""本地复现脚本公共工具：零第三方依赖，仅用标准库驱动 HTTP 接口。

用法（被各场景脚本 import）：
  - 优先读取环境变量 WATER_BASE_URL（默认 http://127.0.0.1:8000）
  - 所有请求都打印方法、路径与关键结果，便于演示观察
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

BASE_URL = os.environ.get("WATER_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# 与 samples/batch_compatible.csv 等价的内置数据，脚本可独立运行
COMPATIBLE_CSV = (
    "样本号,浓度\n"
    "WQ-1001,12.340000\n"
    "WQ-1002,0.000000\n"
    "WQ-1003,\n"
    "WQ-1004,-7.500000\n"
    "WQ-1005,100.000000\n"
)
INCOMPATIBLE_CSV = (
    "样本号,浓度\n"
    "WQ-2001,8.800000\n"
    "WQ-2002,3.145900\n"
    "WQ-2003,\n"
    "WQ-2004,123456789.120000\n"
    "WQ-2005,6.000001\n"
)


def request(
    method: str,
    path: str,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    url = BASE_URL + path
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        body = exc.read()
        status = exc.code
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        parsed = {"raw": body[:200].decode("utf-8", "replace")}
    print(f"{method} {path} -> {status}")
    return {"status": status, "json": parsed}


def get(path: str) -> Dict[str, Any]:
    return request("GET", path)


def post_json(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return request(
        "POST",
        path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


def upload_csv(csv_text: str, name: str = "batch.csv") -> Dict[str, Any]:
    boundary = "----water-demo-boundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode("utf-8") + csv_text.encode("utf-8") + f"\r\n--{boundary}--\r\n".encode()
    return request(
        "POST",
        "/api/batches",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )


def set_fault(point: Optional[str], mode: str = "raise", **extra: Any) -> Dict[str, Any]:
    if point is None:
        return request("DELETE", "/api/admin/faults")
    payload = {"point": point, "mode": mode, "one_shot": True, **extra}
    return post_json("/api/admin/faults", payload)


def start_migration(precision: int, scale: int) -> int:
    qs = urllib.parse.urlencode(
        {"target_precision": precision, "target_scale": scale}
    )
    resp = request("POST", f"/api/migrations?{qs}")
    return int(resp["json"]["migration_id"])


def wait_migration(mid: int, timeout: float = 60.0) -> Dict[str, Any]:
    deadline = time.time() + timeout
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        last = get(f"/api/migrations/{mid}")["json"]
        if last["status"] != "running":
            return last
        time.sleep(0.25)
    raise TimeoutError(f"迁移 {mid} 未在 {timeout}s 内结束: {last}")


def show_schema() -> Dict[str, Any]:
    resp = get("/api/schema")
    sch = resp["json"]
    print(
        f"  当前模式: {sch['concentration_type']} | 活动代 gen-{sch['gen_id']} | "
        f"批次 {sch['batch_count']} | 总行数 {sch['total_rows']}"
    )
    return sch


def banner(title: str) -> None:
    line = "=" * 68
    print(f"\n{line}\n{title}\n{line}")


def ensure_healthy() -> None:
    try:
        h = get("/api/health")
    except Exception as exc:  # noqa: BLE001
        print(f"无法连接 {BASE_URL}：{exc}", file=sys.stderr)
        print("请先启动服务：docker compose up --build", file=sys.stderr)
        sys.exit(2)
    if h["status"] != 200:
        print(f"服务未就绪：{h}", file=sys.stderr)
        sys.exit(2)
