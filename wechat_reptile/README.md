# WeChat Spider (微信公众号全链路自动化采集系统)

本项目是一个工业级的微信公众号文章全自动采集方案，专为大规模数据抓取、高安全性避险及离线化存储需求设计。系统采用生产者-消费者 (Producer-Consumer) 架构，通过数据库原子锁机制实现文章列表同步与详情解析的完全解耦与并行执行。

## 🏗️ 系统架构

本系统由两个相互独立但数据共享的任务组成：

### 列表同步器 (List Synchronizer)
- **角色**：生产者
- **核心功能**：快速扫描目标公众号，获取文章元数据（标题、链接、SN、发布时间），并维护全局登录态
- **避险逻辑**：自动识别已入库数据，发现重复页即触发增量跳过，最大限度保护账号安全

### 详情提取器 (Detail Extractor)
- **角色**：消费者
- **核心功能**：解析正文 HTML，自动下载图片并重命名，生成排版优美的 Markdown 文件
- **避险逻辑**：严格执行单篇随机休眠及任务总寿命管理，模拟真人阅读行为

## 🚀 核心功能

- **双重并发任务锁**：在 `wechat_config` 表中维护 `is_running` 和 `is_detail_running` 状态位，支持列表与详情任务同时启动而互不干扰，新任务检测到旧锁会自动退出并报警
- **全链路异常告警**：深度集成飞书机器人 Webhook。当 Cookie 失效时，系统会自动推送实时消息，并根据配置唤起有头模式浏览器供扫码确认
- **智能路径安全**：代码自动判定 `.env` 配置路径性质（绝对 vs 相对），确保在不同服务器或 OpenClaw 环境迁移后，日志与数据存储路径绝不偏移
- **AI 技能化封装**：提供标准的 `manifest.json` 与 `skill.md`，可作为 OpenClaw 技能直接通过 AI 指令驱动

## 🛠️ 环境配置 (.env)

配置是系统的灵魂，请确保在项目根目录创建 `.env` 并填写：

| 配置分类 | 参数名称 | 说明 |
|---------|---------|------|
| 数据库 | `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | MySQL 连接核心信息 |
| 路径 | `DATA_ROOT`, `LOG_DIR` | 文章存储根目录及日志存放目录 |
| 任务锁 | `is_running`, `is_detail_running` | 数据库字段，由程序自动维护 |
| 随机时长 | `RUN_DURATION_MIN/MAX` | 单次任务执行的总寿命（分钟） |
| 休眠间隔 | `ARTICLE_SLEEP_MIN/MAX` | 单篇文章抓取后的休息时长（秒） |
| 通知 | `FEISHU_WEBHOOK_URL` | 飞书机器人 Webhook 地址 |

## 📂 快速部署指南

### 1. 初始化系统

运行初始化脚本以自动创建数据库表结构（`wechat_config`, `bloggers`, `articles`, `article_details`）及本地必要的文件夹层级：

```bash
python init_setup.py
```

## 2. 配置博主

在数据库的 `bloggers` 表中手动插入或通过 AI 指令录入目标公众号的 `fakeid` 与 `nickname`。

## 3. 并行启动

建议通过定时任务（如 Crontab）每 30 分钟调用一次：

```bash
# 启动列表同步
python scheduler_skill.py

# 启动详情抓取
python wechat_detail_task.py
```
## 📂 存储规范说明

- **Markdown 存储：文件保存在 markdowns/ 下，命名格式为 {文章标题}_{article_sn}.md。**
- **图片归档：图片保存在 images/{日期}/ 下，采用安全重命名策略，防止同名冲突。**
- **日志记录：详细记录启动延迟、单条耗时、任务剩余时长等信息，方便全链路追踪。**