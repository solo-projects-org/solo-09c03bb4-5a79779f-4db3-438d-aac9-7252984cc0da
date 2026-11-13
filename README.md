# 水质浓度 Parquet 精度管理（单实例 Web 服务）

按省库规范调整浓度明细 Parquet 的 `decimal128` 精度。全程**十进制精确存储、不经浮点、保留空值**；
精度迁移在后台重写全部批次，期间上传、查询、下载不中断，且**只有当含中途新批在内的全部值都能
精确表示、且新模式与全套 Parquet 文件都物理就绪时才一起启用**，否则列出不兼容样本并失败，
原表与已接收新批一律保留。写盘错误、登记失败、切换时进程退出都能正确回滚或在重启后恢复。

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
├── catalog.db                # SQLite 登记库（活动代指针、批次、迁移记录+相位，WAL 模式）
└── parquet/
    ├── gen-1/                # 第 1 代：decimal128(18,6)，不可变
    │   └── <batch_id>.parquet
    ├── gen-2/                # 成功迁移后产生的新一代（新模式）
    ├── staging-<迁移id>/     # 迁移暂存区，提交前对外不可见，失败即清理
    └── .trash/               # 删除失败的目录/文件隔离区，绝不阻挡重试
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

### 两阶段提交（修复“失败却已切换、重启不恢复”的关键）

每条迁移记录带一个 `phase` 相位，严格区分“登记已发布”和“物理目录已就绪”：

| 相位 | 含义 | 进程在此阶段退出后，重启如何处理 |
|---|---|---|
| `preparing` | 暂存区重写中，活动代指针仍是旧代 | 迁移判 `failed`、清扫暂存区与 `.tmp`，沿用原表 |
| `publishing` | 登记库事务已提交（新代批次已登记、指针已翻转），目录提升待确认 | 新表完整→续提为 `committed`；不完整→**整体回滚**（删误登记批次、指针恢复旧代、迁移判失败） |
| `committed` | 目录提升并逐文件校验通过，迁移彻底成功 | 已完成，新表继续有效 |
| `failed` | 失败或重启时未完成 | 活动代始终是一套完整表 |

只有**新模式（登记指针）与全套 Parquet（物理目录）同时就绪并校验通过**才会进入 `committed`。
目录提升在登记事务之外执行，一旦失败立即在另一个事务里回滚登记层，因此绝不会停留在
“迁移显示失败、当前模式却已改变、指向缺失目录”的悬空状态（这正是本次修复的核心缺陷）。

### 其它一致性设计

| 关注点 | 设计 |
|---|---|
| 迁移期间不中断 | 迁移在 `staging-<id>` 暂存区后台重写，活动代指针不动；上传/查询/下载始终读当前完整活动代 |
| 中途新批不丢、不漏 | 上传在**写盘前于提交锁内重读活动模式**：未翻转就写旧代，已翻转就写新代；提交瞬间在同一把锁内 drain 新到批次并补拷、补校验 |
| 全量精确才切换 | 提交前对所有批次（含中途新批）逐值做可表示性判定，任一不兼容即整体失败，列出样本号/批次/原因；空表同样走完整两阶段流程 |
| 原子翻转 | 新代批次登记、活动代翻转、置 `publishing` 在**单个 SQLite 事务**内完成，失败整体回滚 |
| 提升后全套校验 | 目录 rename 后逐文件打开校验浓度列模式，缺文件/异类型/残留 `.tmp` 都视为失败并回滚 |
| 不混旧类型文件 | 「切换前开始、切换后完成」的上传，写盘前读到的一定是新模式，物理上只会落进新代目录 |
| 每次只读一套 | 读路径在锁内取活动代指针并只枚举该代目录；登记文件若物理缺失直接返回 503，绝不返回残缺的一套 |
| 写盘不留半成品 | 所有 Parquet 走「唯一临时文件 + fsync + 原子 rename」，崩溃残留 `.tmp` 在启动时清扫 |
| 清理失败不伤人 | 删除“先直接删、失败则移入 `.trash` 隔离”，清理再次失败也不损坏有效数据；重试使用全新暂存目录，不被遗留目录阻挡、不复用残缺文件 |
| 重启恢复 | 启动按相位逐条对账：`preparing` 判失败、`publishing` 续提或回滚、`committed` 保留；并清扫无主暂存区/代目录与孤儿文件 |
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
| GET | `/api/migrations` | 迁移历史（含每条记录的 `phase` 相位） |
| GET | `/api/admin/faults` | 查看故障注入状态（演示用） |
| POST | `/api/admin/faults` | 武装一次性故障（`point`/`mode` 等） |
| DELETE | `/api/admin/faults` | 清除全部故障注入 |

> 故障注入接口与网页卡片仅在 `WATER_FAULT_ENABLED=1`（默认）时可用；
> 生产设 `WATER_FAULT_ENABLED=0` 后接口返回 403、所有注入点变为空操作。

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
| `WATER_FAULT_ENABLED` | `1` | 故障注入总开关，生产设 `0` 关闭 |
| `WATER_FAULT_POINT` | 空 | 启动即武装的注入点（一般留空，运行时用接口/网页配置） |
| `WATER_FAULT_MODE` | `raise` | `raise`（业务内失败/回滚）或 `exit`（`os._exit` 硬退出） |
| `WATER_FAULT_ONE_SHOT` | `1` | 触发一次后自动解除，避免污染后续请求 |

## 7. 本地故障复现脚本（写盘 / 登记失败 / 切换时退出）

`scripts/` 下是**仅依赖 Python 标准库**的复现脚本（用 `python3 -I` 运行，不受当前目录注入影响）。
先启动服务，再按场景运行；脚本默认连 `http://127.0.0.1:8000`，可用 `WATER_BASE_URL` 覆盖。

```bash
docker compose up --build            # 或本地 uvicorn（见第 1 节）

python3 -I scripts/repro_write_failure.py          # ① 迁移暂存写盘错误
python3 -I scripts/repro_registration_failure.py   # ② 上传登记失败 + 迁移登记事务失败
python3 -I scripts/repro_switch_failure.py         # ③ 目录切换失败（含空表场景）
python3 -I scripts/repro_exit_recovery.py          # ④ 真实子进程硬退出 + 重启恢复
```

各脚本都会打印每一步的 HTTP 结果与断言结论：

- **写盘错误**：迁移 `failed`、模式不翻转、原表与全部批次保留，解除后可上传/查询/下载/重新迁移；
- **登记失败**：上传登记失败回滚已写文件；迁移事务失败整体回滚，新代零登记、暂存区清理；
- **切换失败**：模拟“目录发布写盘错误”，验证不再出现“失败却已切换”，重试不被遗留目录阻挡；空表同样覆盖；
- **切换时退出**：脚本拉起真实 uvicorn 子进程，在 `staging_write` 与 `promote_exit` 阶段
  `os._exit(2)` 硬退出，再用同一数据卷重启，验证 `preparing`→判失败、`publishing`→续提或回滚，
  恢复后数据一致且能重新迁移。

故障注入点（也可在网页「④ 故障注入演示」或 `POST /api/admin/faults` 配置）：

| 注入点 | 触发位置 |
|---|---|
| `staging_write` | 迁移暂存区批次 Parquet 写盘前 |
| `db_commit` | 迁移登记库翻转事务提交前 |
| `promote` | 暂存目录提升为正式代目录前（抛错，走回滚） |
| `promote_exit` | 同一位置直接 `os._exit`（走重启对账） |
| `staging_cleanup` | 失败后清理暂存目录时（验证清理再失败不损数据） |
| `upload_write` / `upload_register` / `upload_cleanup` | 上传写盘 / 登记 / 回滚清理三个环节 |

## 8. 测试

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -q
```

覆盖：精确十进制与空值保留、非法 CSV/超范围拒绝、失败迁移保留原表与新批、成功切换全套文件、
迁移中兼容/不兼容并发上传、跨切换上传不混旧类型、下载包模式一致性、迁移中下载只读旧完整代、
单迁移互斥、重启崩溃恢复（preparing/publishing 两相位的判失败、续提与回滚）、迁移历史累积、
空表迁移、写盘/登记/切换故障注入、清理再失败隔离、上传三环节故障回滚、故障开关与管理接口。

## 9. 目录结构

```
app/
├── main.py            # FastAPI 应用工厂与路由（含故障管理接口、503 一致性处理）
├── config.py          # 环境变量配置
├── catalog.py         # SQLite 登记层（活动代指针、批次、迁移相位与两阶段事务）
├── migration.py       # 迁移编排、并发锁、暂存/drain/两阶段提交、按相位崩溃恢复、孤儿对账
├── storage.py         # 代目录与 decimal128 Parquet 原子读写、隔离清理、完整性校验
├── faults.py          # 故障注入器（写盘/登记/切换/退出，支持 raise 与 os._exit）
├── csv_utils.py       # 两列 CSV 解析
├── decimal_utils.py   # Decimal 解析与可表示性判定
└── static/index.html  # 单页控制台（含故障注入演示卡片与迁移相位展示）
samples/               # 演示 CSV
scripts/               # 仅用标准库的本地故障复现脚本（4 个场景）
tests/test_app.py      # 端到端行为测试
tests/test_faults.py   # 故障注入与崩溃恢复测试
docker-compose.yml / Dockerfile / requirements.txt
```
