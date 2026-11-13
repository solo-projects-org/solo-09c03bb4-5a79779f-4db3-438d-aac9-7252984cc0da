"""FastAPI 应用工厂：上传 / 查询 / 下载 / 迁移，全部围绕活动代一致性快照。"""
from __future__ import annotations

import io
import json
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from .catalog import create_catalog
from .config import Settings
from .csv_utils import CsvValidationError, parse_csv
from .migration import MigrationRunningError, create_migration_service
from .storage import CONC_FIELD, SAMPLE_FIELD, read_parquet


def create_app(settings: Settings) -> FastAPI:
    # 数据卷目录先建好，Catalog 连接与建表才不会因目录缺失失败
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.parquet_root.mkdir(parents=True, exist_ok=True)

    catalog = create_catalog(settings.db_path)
    service = create_migration_service(
        catalog, settings.parquet_root, settings.MIGRATION_DELAY_SECONDS
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 启动：崩溃恢复（遗留 running 判失败）+ 初代初始化
        service.startup()
        try:
            yield
        finally:
            # 先等待在途迁移线程结束，再关闭数据库，杜绝游离后台线程触库
            service.shutdown()
            catalog.close()

    app = FastAPI(
        title="水质浓度 Parquet 精度管理",
        description="按省库规范调整浓度明细 decimal128 精度，迁移期间不中断上传/查询/下载。",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.catalog = catalog
    app.state.service = service

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html = (Path(__file__).parent / "static" / "index.html").read_text(
            encoding="utf-8"
        )
        return HTMLResponse(html)

    @app.get("/api/health")
    def health(request: Request) -> dict:
        svc = request.app.state.service
        active = svc.active_snapshot()
        running = svc.current_migration()
        return {
            "status": "ok",
            "active_generation": active["gen_id"],
            "schema": f"decimal128({active['precision']},{active['scale']})",
            "migration_running": running is not None,
        }

    @app.get("/api/schema")
    def get_schema(request: Request) -> dict:
        svc = request.app.state.service
        snap = svc.active_snapshot()
        return {
            "gen_id": snap["gen_id"],
            "precision": snap["precision"],
            "scale": snap["scale"],
            "activated_at": snap["activated_at"],
            "concentration_type": f"decimal128({snap['precision']},{snap['scale']})",
            "batch_count": len(snap["batches"]),
            "total_rows": sum(b["row_count"] for b in snap["batches"]),
        }

    @app.post("/api/batches")
    async def upload_batch(
        request: Request, file: UploadFile = File(...)
    ) -> dict:
        svc = request.app.state.service
        content = await file.read()
        if len(content) > settings.MAX_UPLOAD_BYTES:
            raise HTTPException(413, "CSV 文件超过大小上限")
        try:
            samples, concentrations = parse_csv(content)
        except CsvValidationError as exc:
            raise HTTPException(400, f"CSV 校验失败：{exc}") from exc
        try:
            return svc.upload_batch(samples, concentrations)
        except Exception as exc:
            raise HTTPException(400, f"批次写入失败：{exc}") from exc

    @app.get("/api/batches")
    def list_batches(request: Request) -> dict:
        svc = request.app.state.service
        snap = svc.active_snapshot()
        return {
            "gen_id": snap["gen_id"],
            "precision": snap["precision"],
            "scale": snap["scale"],
            "batches": snap["batches"],
        }

    @app.get("/api/rows")
    def query_rows(
        request: Request,
        sample_id: Optional[str] = Query(None, description="按样本号精确过滤"),
        limit: int = Query(100, ge=1, le=10000),
        offset: int = Query(0, ge=0),
    ) -> dict:
        """只读当前活动代这一套完整模式与批次，返回浓度的精确十进制字符串。"""
        svc = request.app.state.service
        snap = svc.active_snapshot()
        rows: List[dict] = []
        total_matched = 0
        for f in snap["files"]:
            table = read_parquet(f)
            samples = table.column(SAMPLE_FIELD).to_pylist()
            concs = table.column(CONC_FIELD).to_pylist()
            for s, c in zip(samples, concs):
                if sample_id is not None and s != sample_id:
                    continue
                total_matched += 1
                if len(rows) < limit and total_matched - 1 >= offset:
                    rows.append(
                        {
                            "batch_id": f.stem,
                            SAMPLE_FIELD: s,
                            # Decimal -> str，序列化为文本，杜绝前端/JSON 浮点化
                            CONC_FIELD: None if c is None else str(c),
                        }
                    )
        return {
            "gen_id": snap["gen_id"],
            "schema": f"decimal128({snap['precision']},{snap['scale']})",
            "total_matched": total_matched,
            "limit": limit,
            "offset": offset,
            "rows": rows,
        }

    @app.get("/api/download")
    def download_package(request: Request) -> StreamingResponse:
        """打包当前活动代的全套 Parquet（同一模式）+ 清单，始终是完整一致的一套。"""
        svc = request.app.state.service
        snap = svc.active_snapshot()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            manifest = {
                "gen_id": snap["gen_id"],
                "concentration_type": f"decimal128({snap['precision']},{snap['scale']})",
                "precision": snap["precision"],
                "scale": snap["scale"],
                "activated_at": snap["activated_at"],
                "batches": snap["batches"],
            }
            zf.writestr(
                "manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2)
            )
            for f in snap["files"]:
                zf.write(f, arcname=f"parquet/{f.name}")
        buffer.seek(0)
        headers = {
            "Content-Disposition": (
                f'attachment; filename="parquet-gen{snap["gen_id"]}'
                f'-p{snap["precision"]}s{snap["scale"]}.zip"'
            )
        }
        return StreamingResponse(
            buffer, media_type="application/zip", headers=headers
        )

    @app.post("/api/migrations")
    def create_migration(
        request: Request,
        target_precision: int = Query(..., ge=1, le=38),
        target_scale: int = Query(..., ge=0, le=38),
    ) -> JSONResponse:
        svc = request.app.state.service
        if target_scale > target_precision:
            raise HTTPException(422, "小数位数不能大于总位数")
        try:
            result = svc.start_migration(target_precision, target_scale)
        except MigrationRunningError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return JSONResponse(result, status_code=202)

    @app.get("/api/migrations/current")
    def current_migration(request: Request) -> dict:
        svc = request.app.state.service
        running = svc.current_migration()
        return {"running": running is not None, "migration": running}

    @app.get("/api/migrations/{migration_id}")
    def migration_status(request: Request, migration_id: int) -> dict:
        svc = request.app.state.service
        row = svc.migration_status(migration_id)
        if row is None:
            raise HTTPException(404, "迁移记录不存在")
        return row

    @app.get("/api/migrations")
    def all_migrations(request: Request) -> dict:
        svc = request.app.state.service
        return {"migrations": svc.list_migrations()}

    return app


def __getattr__(name: str):
    # 惰性默认实例：仅在 uvicorn 以 app.main:app 加载时才按环境变量创建，
    # 避免 import 阶段触碰文件系统（测试用 create_app 显式装配）。
    if name == "app":
        return create_app(Settings())
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
