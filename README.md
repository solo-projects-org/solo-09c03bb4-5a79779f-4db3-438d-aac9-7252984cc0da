# 水质浓度 Parquet 精度管理（单实例 Web 服务）

按省库规范调整浓度明细 Parquet 的 `decimal128` 精度。全程**十进制精确存储、不经浮点、保留空值**；
精度迁移在后台重写全部批次，期间上传、查询、下载不中断，且**只有当含中途新批在内的全部值都能
精确表示时才一起启用新模式与全套文件**，否则列出不兼容样本并失败，原表与已接收新批一律保留。

技术栈：FastAPI + PyArrow + SQLite，单实例、单进程、Docker Compose 启动，数据落数据卷。

---

## 1. 快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据全部写入命名卷 `water-data`（容器内 `/data`）：

```
/data
├── catalog.db                # SQLite 登记库（活动代指针、批次、迁移记录，WAL 模式）
└── parquet/
    ├── gen-1/                # 第 1 代：decimal128(18,6)，不可变
    │   └── <batch_id>.parquet
    ├── gen-2/                # 成功迁移后产生的新一代（新模式）
    └── staging-<迁移id>/     # 迁移暂存区，提交前对外不可见，失败即清理
```

本地无 Docker 时（需 Python 3.11）：

```bash
pip install -r requirements.txt
WATER_DATA_DIR=./data MIGRATION_DELAY_SECONDS=1 \
  uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

> 必须 `--workers 1`：迁移协调依赖进程内锁，多 worker 会破坏单实例语义。

---

## 2. 核心数据规则

- 上传 CSV **仅允许两列**：`样本号,浓度`（兼容 `sample_id/concentration` 等英文别名，支持 UTF-8 BOM）。
- 浓度用 `decimal.Decimal` 解析，再由 PyArrow 直接写成 `decimal128`，**任何环节都不经过 float/double**。
- 初始模式固定 `decimal128(18,6)`；空单元格保留为 `null`，拒绝 `NaN/Inf`、非法十进制与超范围值。
- 查询接口把浓度以**十进制字符串**返回，避免前端/JSON 数值化引入误差。
- 每个批次写盘走「临时文件 + 原子 rename」，登记失败会回滚已写文件，不留半结果。

## 3. 迁移一致性是怎么保证的

| 关注点 | 设计 |
|---|---|
| 迁移期间不中断 | 迁移在 `staging-<id>` 暂存区后台重写，活动代指针不动；上传/查询/下载始终读当前完整活动代 |
| 中途新批不丢、不漏 | 上传在**写盘前于提交锁内重读活动模式**：未翻转就写旧代，已翻转就写新代；提交瞬间在同一把锁内 drain 新到批次并补拷、补校验 |
| 全量精确才切换 | 提交前对所有批次（含中途新批）逐值做可表示性判定，任一不兼容即整体失败，列出样本号/批次/原因 |
| 原子翻转 | 翻转活动代、登记新代全部批次、迁移置 `committed` 在**单个 SQLite 事务**内完成，失败整体回滚 |
| 不混旧类型文件 | 「切换前开始、切换后完成」的上传，写盘前读到的一定是新模式，物理上只会落进新代目录 |
| 每次只读一套 | 读路径在锁内取活动代指针并只枚举该代目录，查询/下载拿到的永远是一套自洽的模式与批次 |
| 写盘/登记失败不暴露半成品 | 暂存区提交前不可见；失败清理暂存目录并标记迁移 `failed`，活动代不变 |
| 重启恢复 | 启动时把所有遗留 `running` 迁移标记为 `failed` 并清理暂存区；已 `committed` 的新表继续有效 |
| 同时只迁一次 | 存在 `running` 迁移时再次发起返回 `409` |

可表示性判定（`app/decimal_utils.py::representable`）与 PyArrow `safe cast` 同口径，
已用 **6 万组随机 + 负值/零值用例**交叉验证零偏差：小数位收窄时超出尾数必须全为 0，
整数位数 + scale 不得超过 precision。

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 网页控制台（上传、迁移、查询、下载、迁移历史） |
| GET | `/api/health` | 健康检查 + 当前模式 + 是否有迁移在跑 |
| GET | `/api/schema` | 当前活动代模式、批次数、总行数 |
| POST | `/api/batches` | 上传两列 CSV（multipart 字段 `file`） |
| GET | `/api/batches` | 列出当前活动代批次 |
| GET | `/api/rows?sample_id=&limit=&offset=` | 查询浓度（精确十进制文本，空值为 null） |
| GET | `/api/download` | 下载当前活动代**全套** Parquet + `manifest.json` 的 zip |
| POST | `/api/migrations?target_precision=&target_scale=` | 发起后台迁移（202） |
| GET | `/api/migrations/current` | 当前运行中的迁移 |
| GET | `/api/migrations/{id}` | 单个迁移状态（失败时含不兼容样本清单） |
| GET | `/api/migrations` | 迁移历史 |

## 5. 网页演示：在迁移中追加不兼容浓度

`samples/` 提供两份样例（网页上也有同名「载入样例」按钮）：

- `batch_compatible.csv`：对 `decimal128(18,2)` 全部可精确表示。
- `batch_incompatible.csv`：含 `3.145900`、`6.000001`（收窄到 2 位小数会丢精度）。

演示脚本（`MIGRATION_DELAY_SECONDS=1` 让每个批次重写停顿 1 秒，留出操作窗口）：

1. 打开 `http://localhost:8000`，上传 `batch_compatible.csv`（可多传几批，窗口更长）。
2. 目标精度填 `18`、小数位填 `2`，点「启动迁移」。
3. **迁移进行中**，在上传区点「载入不兼容浓度样例」→「上传」。该批按旧模式 `(18,6)` 被正常接收。
4. 迁移结束：状态为 **失败**，页面列出不兼容样本（如 `WQ-2002 / 3.145900`）；
   当前模式仍是 `decimal128(18,6)`，原表与刚上传的新批都能查到，暂存区已清理。
5. 再发起一次目标为 `20 / 6`（加宽、不丢小数）的迁移 → **成功**，活动代切到 `gen-2`，
   点「下载当前表 Parquet 包」，zip 内每个 Parquet 的浓度列都是新模式，无旧类型文件混入。

命令行等价操作：

```bash
curl -F "file=@samples/batch_compatible.csv;type=text/csv" \
  http://localhost:8000/api/batches
curl -X POST "http://localhost:8000/api/migrations?target_precision=18&target_scale=2"
# 迁移窗口内：
curl -F "file=@samples/batch_incompatible.csv;type=text/csv" \
  http://localhost:8000/api/batches
curl http://localhost:8000/api/migrations/1      # 查看失败原因与不兼容样本
curl -OJ http://localhost:8000/api/download      # 下载当前完整 Parquet 包
```

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `WATER_DATA_DIR` | `/data` | 数据卷根目录（SQLite + Parquet） |
| `MIGRATION_DELAY_SECONDS` | `0.3` | 迁移每批次重写停顿，演示用；生产可设 `0` |
| `MAX_UPLOAD_BYTES` | `16777216` | 单个 CSV 上传上限（16 MiB） |

## 7. 测试

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -q
```

覆盖：精确十进制与空值保留、非法 CSV/超范围拒绝、失败迁移保留原表与新批、成功切换全套文件、
迁移中兼容/不兼容并发上传、跨切换上传不混旧类型、下载包模式一致性、迁移中下载只读旧完整代、
单迁移互斥、重启崩溃恢复、迁移历史累积。

## 8. 目录结构

```
app/
├── main.py            # FastAPI 应用工厂与路由
├── config.py          # 环境变量配置
├── catalog.py         # SQLite 登记层（活动代指针、批次、迁移事务）
├── migration.py       # 迁移编排、并发锁、暂存/drain/原子提交、崩溃恢复
├── storage.py         # 代目录与 decimal128 Parquet 读写、安全转型
├── csv_utils.py       # 两列 CSV 解析
├── decimal_utils.py   # Decimal 解析与可表示性判定
└── static/index.html  # 单页控制台
samples/               # 演示 CSV
tests/test_app.py      # 端到端测试
docker-compose.yml / Dockerfile / requirements.txt
```
