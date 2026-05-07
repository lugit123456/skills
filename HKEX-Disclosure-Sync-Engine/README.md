# HKEX Disclosure Sync Skill (港交所披露权益同步引擎)

这是一个专为金融数据采集设计的工业级爬虫引擎。它能够自动处理港交所（HKEX）披露权益（DI）数据的历史全量补课与每日增量同步，具备强大的避险机制和自动化的日志管理功能。

## 1. 核心特性

* **⚡ 双模式自动切换**：系统默认以 `BULK`（全量抓取）模式运行，当爬取进度触达数据库设定的终点日期后，会自动切换为 `INCREMENTAL`（每日增量）模式，实现零人工干预运维。
* **🛡️ 增强型去重**：通过全字段复合指纹算法（包含序号、申报人、法团、股数、关联链接等），彻底解决港交所页面中"关联子记录"和"无序号行"的遗漏问题。
* **📅 即时日期日志**：日志系统自动生成带日期的文件名（如 `disclosure_sync_2026-05-07.log`），并严格执行 7 天滚动保留策略，自动清理旧日志。
* **🔒 安全避险机制**：内置原子化任务锁（防止并发冲突）、随机任务寿命（Session Lifespan）以及模拟真实浏览器的 SSL 指纹避险（TlsAdapter）。
* **📊 结构化存储**：提供标准化的日期转换功能，将港交所原始字符串日期转换为数据库 `DATE` 类型，极大提升筛选效率。

## 2. 目录结构

```text
hkex-disclosure-skill/
├── manifest.json                # Skill 元数据及依赖声明
├── init_setup.py                # 数据库及表结构一键初始化脚本
├── disclosure_sync_manager.py   # 核心同步引擎（主入口）
├── logs/                        # 日志存放目录（自动生成，保留最近7天）
├── .env                         # 环境配置文件（数据库连接、避险参数）
├── README.md                    # 本用户手册
└── SKILL.md                     # 技术开发与架构设计文档
```

## 3. 部署与安装

### 第一步：安装依赖库

确保您的系统已安装 Python 3.9+，执行以下命令安装必要组件：

```bash
pip install requests pandas beautifulsoup4 mysql-connector-python python-dotenv lxml html5lib
```

### 第二步：配置环境变量

在项目根目录创建 `.env` 文件，并根据您的 MySQL 环境进行配置：

```env
# 数据库连接配置
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASS=your_password
DB_NAME=hkex_data

# 避险配置，每次抓取总时间区间 (单位: 秒)
SESSION_LIFESPAN_MIN=1500
SESSION_LIFESPAN_MAX=1600

# 避险配置，每抓取一页后休眠时间区间 (单位: 秒)
ITEM_SLEEP_MIN=2
ITEM_SLEEP_MAX=6
```

### 第三步：初始化数据库

运行初始化脚本。它将自动创建数据库、数据表，并插入初始同步种子（默认从 2003-01-01 开始全量抓取）。

```bash
python init_setup.py
```

## 4. 使用说明

### 启动同步

直接运行主程序即可启动：

```bash
python disclosure_sync_manager.py
```

### 运行逻辑说明

**全量抓取模式 (BULK)**：程序启动后，会从 `crawler_status` 表读取当前窗口和页码。每完成一页都会实时保存断点。当所有历史数据爬取完毕，模式将自动改为 `INCREMENTAL`。

**增量同步模式 (INCREMENTAL)**：在此模式下，程序每天只抓取最近 3 天的数据。一旦在页内探测到已存在的记录（基于官方序号），将认为已追平进度，自动停止本次任务。

## 5. 日志与维护

### 日志查看

日志保存在 `./logs/` 文件夹下。

- **文件名格式**：`disclosure_sync_YYYY-MM-DD.log`
- **自动清理**：系统仅保留最近 7 天的日志，过期的日志文件会被自动物理删除，无需手动维护磁盘空间。

### 手动干预

如果需要对抓取任务进行手动调整，请直接操作数据库中的 `crawler_status` 表：

- **强制重爬历史**：将 `last_mode` 字段改回 `BULK`，并设置 `current_window_start` 为起始日期。
- **修改爬取终点**：修改 `global_target_end` 字段。
- **解除异常锁**：若程序意外崩溃导致无法启动，将 `is_running` 改为 `0`。

## 6. 技术支持

- **数据一致性**：使用 `INSERT IGNORE` 配合 `fingerprint` 唯一索引。
- **SSL 绕过**：通过自定义 `TlsAdapter` 实现。

> **注意**：请遵守港交所相关数据使用政策，建议在凌晨非高峰时段设置 Cron Job 定时运行增量同步。
