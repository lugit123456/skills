# Alphapai Reptile Skill

本技能包由 OpenClaw 驱动，专门用于抓取和管理 Alphapai 平台的研报、会议及公告数据。

## 核心特性
1. **智能去重**: 采用 `post_id` 和 `title` 双重查重机制，解决研报 ID 随机漂移问题。
2. **会议解析**: 自动提取 `mtSummarySwitchOpen` 正文，并合并 `aiSummaryV3` 摘要。
3. **文件本地化**: 自动重命名附件（格式：`标题_时间.pdf`），按日期目录存储。
4. **增量/全量切换**: 自动识别抓取进度，完成后自动转入增量监控模式。

## 快速上手
1. 配置项目根目录下的 `.env` 文件。
2. 运行初始化脚本：`python3 init_setup.py`。
3. 在 `crawl_keywords` 表中插入你感兴趣的关键词（如 `德福科技`, `光模块`）。
4. 运行列表抓取：`python3 list_crawler.py`。
5. 运行详情抓取：`python3 detail_crawler.py`。

## 注意事项
- 下载的图片及 PDF 默认保存在 `DATA_ROOT` 路径下。
- 详情抓取建议在深夜执行，或通过 `.env` 设置合理的休眠时间。