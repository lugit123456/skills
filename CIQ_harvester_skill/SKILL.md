# ciq_financial_data_extractor

> S&P Capital IQ 财务与预测数据全自动提取技能

本文件为 `ciq_financial_data_extractor` 技能的技术文档与大模型（LLM）Prompt 路由指南。主要用于指导 AI Agent 理解该工具的边界限制、参数传递规则以及底层运作机理。

---

## ⚠️ 核心平台限制与大模型前置路由规则 (CRITICAL REGULATION)

### 1. 强制中英实体转换 (Language Constraint & Translation)

* **硬性缺陷**：S&P Capital IQ 平台的底层 `OmniSearch API` 核心检索引擎**不支持中文搜索**。若直接使用中文公司名称（如 "英伟达"、"苹果公司"）作为关键词发起请求，后台会返回空结果，导致脚本因无法解析 `hitId` 而中断。

* **Agent 路由准则**：大模型（LLM）在解析用户输入的自然语言并组装 `company` 参数时，**必须自动执行前置翻译与实体标准化逻辑**。

| 用户输入 | Agent 转换结果 |
|---------|--------------|
| "帮我下**英伟达**的财报" | `company="Nvidia"` |
| "把**微软**的业绩指引存成 PDF" | `company="Microsoft"` |
| "搜一下 **贵州茅台**" | `company="Kweichow Moutai"` |

### 2. 调度控制与参数矩阵 (Parameter Routing)

大模型调用本技能时，请严格按照以下逻辑组装 CLI 命令：

* **`company`**: 必须是翻译后的官方英文商用名或股票代码（如 `Apple`、`TSLA`）。
* **`format` (`-f`)**: 动态提取用户需求格式。若用户提到"PDF"，则传入 `-f pdf`；若提到"Word"，则传入 `-f word`；默认且未提及时，传入 `-f pdf`。

**执行命令示例：**

```bash
python search_and_download.py "Nvidia" -f excel
```

---

## 🧬 底层技术架构与运行机理 (Technical Architecture)

本技能采用了"动态 API 嗅探 + 无痕双轨运行时"的现代全自动架构，完美解决了传统网页录制脚本的易错与崩溃问题：

### 1. 认证状态探针 (Authentication Probe)

脚本启动时，会优先加载本地的会话缓存 `ciq_cookies.json`。

在不唤起浏览器的情况下，通过向后台静默发送轻量级 OData 检索请求（探针）来验证 Cookie 是否存活。若有效，则直接跳过繁琐的 UI 登录步骤，节约 80% 的前期开销。

### 2. 树状菜单 API 递归逆向 (Menu Parsing)

绕过网页前端的 DOM 结构限制，直接请求底层的 LeftNavigationMenu API。

深度遍历 JSON 结构，精准抽取"财务数据"大类下的所有报表，以及"预测数据"大类下的"业绩指引"、"一致预测"等独立模块，生成干净的目标 URL 任务队列。

### 3. "阅后即焚"式单页隔离 (Lifecycle Management)

针对企业级单页应用（SPA）极其严重的内存泄漏机制，本引擎采用了每执行一个下载任务，便新建一个干净的 Page 标签页，用完即毁的策略。

100% 避免了在同一页面频繁跳转导致的 `Frame was detached`（网页内嵌框架断开连接）毁灭性崩溃报错。

### 4. 穿透型 DOM 扫描与强行注入 (DOM Penetration)

* **iframe 自动穿透**：脚本会动态扫描 `[page] + page.frames` 构成的全量沙箱框架阵列，即使数据表格被深度嵌套也能精准锁定。
* **原生点击注入**：废弃了易被透明 Loading 蒙层拦截的物理鼠标模拟，直接调用 `evaluate("node => node.click()")` 从底层 JS 内核强行下发事件。

---

## 📈 失败自愈补偿回路 (Self-Healing Framework)

脚本内置两阶段容错网络，以应对金融级专线网络的瞬时抖动：

### 初轮全量扫描

以 3-8 秒的拟人化随机休眠速度推进。遇到超时、假死时，直接捕获异常并存入 `failed_tasks` 任务池，绝不中断主进程。

### 终轮专项补漏

全量跑完后，若失败池不为空，主进程将挂起 5 秒，随后唤起"自愈补漏流"，对失败项给予更高的加载容限重新抓取，确保最终交付产物零遗漏。

---

## 🛠️ 运维排障手册 (Troubleshooting Matrix)

### 1. 频繁提示状态已过期，登录陷入超时

**原因**：CIQ 检测到异地登录异常，触发了多因素认证（MFA / 验证码），无头模式无法自动通过。

**排障**：激活环境并在终端带上 `--headed` 参数手动运行一次：

```bash
python search_and_download.py "Apple" -f excel --headed
```

手动完成 MFA 或验证码验证后，新的会话 Cookie 会被自动写入 `ciq_cookies.json`，后续即可恢复无头全自动模式。

### 2. 下载结果为空或文件损坏

**原因**：目标报表的 DOM 结构发生变更，或 CIQ 后台对特定报表启用了新的反爬策略。

**排障**：

1. 检查 `logs/` 目录下的运行日志，定位具体失败的报表 URL。
2. 临时开启 `--headed` 模式，人工观察目标页面的加载时序与 iframe 嵌套层级。
3. 根据实际 DOM 结构调整 `config/selectors.yml` 中的 CSS Selector 映射表。

### 3. 一致预测（Consensus）数据缺失

**原因**：CIQ 的 Consensus 模块依赖第三方数据供应商的实时推送，部分新兴市场公司可能存在数据延迟或覆盖空白。

**排障**：

* 在命令中追加 `--include-consensus=false` 跳过该模块。
* 或手动通过 CIQ 网页版确认该公司是否确实存在 Consensus 覆盖。

---

## 📋 参数速查表 (Quick Reference)

| 参数 | 简写 | 必填 | 默认值           | 说明 |
|------|------|------|---------------|------|
| `company` | - | ✅ | -             | 目标公司英文名称或股票代码 |
| `--format` | `-f` | ❌ | `pdf`         | 输出格式：`excel` / `pdf` / `word` |
| `--headed` | - | ❌ | `false`       | 是否启用有头浏览器模式（用于调试/MFA） |
| `--include-consensus` | - | ❌ | `true`        | 是否下载一致预测数据 |
| `--output-dir` | `-o` | ❌ | `./downloads` | 下载文件保存目录 |
| `--max-retries` | - | ❌ | `3`           | 单任务最大重试次数 |
| `--timeout` | - | ❌ | `60`          | 页面加载超时时间（秒） |

---

## 🔒 安全与合规声明

* 本技能仅用于合法授权的企业内部财务分析场景。
* 严禁将提取的数据用于未经授权的商业再分发或公开市场传播。
* 请遵守 S&P Capital IQ 平台的服务条款与数据使用协议。

---

*文档版本：v3.0 | 最后更新：2026-05-16*
