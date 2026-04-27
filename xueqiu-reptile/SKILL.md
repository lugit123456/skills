# 雪球舆情自动化采集 Skill (Xueqiu Smart Skill)

## 1. 核心设计逻辑

本工具采用 **"浏览器环境内异步透传 (In-Page Fetch)"** 方案，通过真实的 Chromium 环境绕过雪球复杂的 WAF 防护。

## 2. 抓取模态

- **全量模态 (Full Sync)**: 针对新博主（`status < 2`），根据数据库记录的断点页码，向上翻页直至历史尽头。
- **增量模态 (Incremental)**: 针对已完成博主（`status = 2`），仅刷新首屏。配合"哨兵机制"，若发现无新帖则秒级切换下一任务。

## 3. 调度算法

工具在每一轮 Agent 启动时，遵循以下优先级执行且**不重复抓取**：

1. `status` 升序（先处理未完成的博主）。
2. `priority` 降序（优先级数字越大，越优先被爬取）。
3. `last_crawl_time` 升序（最久未刷新的博主排前面）。

## 4. 容错与告警

- **三连败保护**: 连续 3 次 Fetch 失败将触发保护，自动截取当前屏幕并保存。
- **飞书通知**: 拦截异常将实时推送至飞书 Webhook。
- **时限闭环**: 到达运行限时前，会先执行数据总量同步统计，再安全关闭数据库与浏览器。

## 5. 环境搭建与首次运行

### 5.1 基础环境配置

1. **配置 `.env`**
   - 复制 `.env.example` 并重命名为 `.env`。
   - 填入 MySQL 连接信息（默认端口 `43306`）和飞书 Webhook 链接。

2. **数据库建表**
   - 在 `anshu_local` 库中运行以下 SQL 脚本，创建 `blogger_tasks` 和 `blogger_posts` 表：

```sql
-- 博主任务表：管理待爬取的博主及其状态
CREATE TABLE `blogger_tasks` (
  `user_id` bigint NOT NULL COMMENT '博主UID',
  `screen_name` varchar(100) DEFAULT NULL COMMENT '博主名称',
  `priority` int DEFAULT '0' COMMENT '优先级',
  `last_crawl_time` datetime DEFAULT NULL COMMENT '最后抓取时间，数字越大优先级越高',
  `status` tinyint DEFAULT '0' COMMENT '0:待处理, 1:抓取中, 2:全量已完成',
  `added_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `checkpoint_page` int DEFAULT '0' COMMENT '当前抓取进度页码',
  `total_posts_count` int DEFAULT '0' COMMENT '已抓取总数统计',
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='雪球博主采集任务表';

-- 帖子内容表：存储抓取到的博文数据
CREATE TABLE `blogger_posts` (
  `id` bigint NOT NULL COMMENT '评论ID',
  `user_id` bigint DEFAULT NULL COMMENT '博主UID',
  `screen_name` varchar(100) DEFAULT NULL COMMENT '博主名称',
  `content` text COMMENT '清洗后的评论内容',
  `stock_codes` varchar(255) DEFAULT NULL COMMENT '股票代码列表',
  `stock_names` varchar(255) DEFAULT NULL COMMENT '股票名称列表',
  `comment_time` datetime DEFAULT NULL COMMENT '博主发布时间',
  `added_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据入库时间',
  `ip_location` varchar(50) DEFAULT NULL COMMENT 'IP属地',
  `raw_json` json DEFAULT NULL COMMENT '原始完整数据',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='雪球博主帖子内容表';
```

### 5.2 任务下发

- 在 `blogger_tasks` 表中插入目标博主 UID 以添加监控任务。
- **示例 SQL**:
  ```sql
  INSERT INTO `blogger_tasks` (`user_id`, `screen_name`, `priority`, `last_crawl_time`, `status`, `added_at`, `checkpoint_page`, `total_posts_count`) VALUES (1039527614, 'DrChuck', 0, '2026-04-27 15:56:35', 0, '2026-04-24 17:34:56', 0, 0);
  ```

### 5.3 完成首次登录（必须）

- **动作**: 对 Agent 说：**"启动雪球采集助手，开启调试模式"**。
- **操作**: 在弹出的 Chrome Testing 窗口中手动登录雪球账号，并完成第一次滑动验证。
- **意义**: 成功后，登录状态会固化在 `xueqiu_user_data` 文件夹中，后续即可实现全自动"无头"运行。
- ⚠️ 注意：本 Skill 的正确启动入口是 XueqiuRandomTrigger().run()，而非直接调用 XueqiuSmartSkill.execute()。建议通过 Trigger 类启动以确保随机逻辑生效。

## 6. 关键参数说明

| 参数 | 说明 |
|------|------|
| `run_minutes` | 建议不要设置太短，因为任务间有 30–60s 的随机防封休眠。 |
| `debug_mode` | 当收到飞书告警提示"需要验证"时，请手动开启此模式以介入处理。 |
| `priority` | 数值越大，该博主在调度队列中的优先级越高。 |

## 7. 进阶逻辑

- **断点续传**: 全量爬取过程中若因超时停止，下次会自动从数据库记录的最后抓取页码（`last_page`）继续，无需从头开始。
- **状态流转**: 新博主默认 `status = 0`，全量同步中置为 `status = 1`，完成后自动置为 `status = 2`，后续进入增量刷新模式。
- **去重机制**: `blogger_posts` 表以 `article_id` 为唯一键，重复文章会自动忽略，避免数据冗余。

## 8. 故障排查与 Agent 指引

### 8.1 初次使用引导

若用户询问如何开始，按以下顺序引导：
1. 在数据库执行 SQL 建表脚本（见 5.1）。
2. 在 `blogger_tasks` 表中插入目标博主 UID。
3. 对 Agent 说："启动雪球采集调试模式"，进行首次人工登录。

### 8.2 常见报错速查

| 报错提示 | 原因 | 解决方案 |
|----------|------|----------|
| "环境未初始化" | 尚未完成首次手动登录 | 执行第 5.3 步，通过 `debug_mode=True` 登录并过验证 |
| "任务表为空" | 尚未添加监控博主 | 执行第 5.2 步，向 `blogger_tasks` 表插入数据 |
| 收到飞书截图告警 / 触发验证码 | 雪球 WAF 拦截 | 对 Agent 说："开启调试模式运行雪球采集"，手动介入过验证 |
| 配置相关报错 | `.env` 文件缺失或填写错误 | 检查 Skill 目录下的 `.env` 文件，确认 MySQL 和 Webhook 配置正确 |

### 8.3 验证码 / WAF 处理

如果采集任务报错或收到飞书截图告警，引导用户通过 `debug_mode=True` 手动介入，完成滑动验证后继续采集。
