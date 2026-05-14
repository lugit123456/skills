# Capital IQ (CIQ) 财务报表下载 Skill

让 AI Agent 帮你在 [S&P Capital IQ](https://www.capitaliq.spglobal.com/) 上通过底层 API 搜索并自动批量下载研报、财报、路演纪要等金融文档。

> 本 Skill 采用混合架构：使用 Playwright 处理复杂的 Okta SSO 登录，并使用底层 OData 接口实现免前端渲染的极速并发下载。

---

## 🚀 安装

把下面这段话发给你的 Agent：

> 请帮我安装 Capital IQ 搜索技能。请克隆仓库到本地的 skills 目录下，并运行 `setup_env.sh` 初始化环境。

Agent 会自动完成以下步骤：
1. 下载代码到本地。
2. 安装 Playwright 及其内置的 Chromium 浏览器内核。
3. 创建 Python 虚拟环境并安装 `requests`, `playwright`, `python-dotenv` 等核心依赖。

---

## 🔑 配置账号

安装完成后，你需要配置你的 CIQ 账号（通常是 Okta 关联账号）。对 Agent 说：

> 请帮我配置 Capital IQ 的登录信息，邮箱是 xxx@example.com，密码是 xxx

Agent 会把账号信息安全地写入 `.env` 文件。

### .env 变量说明

| 变量名 | 是否必填 | 说明 |
|--------|---------|------|
| `CIQ_USERNAME` | ✅ 必填 | 你的 Capital IQ / Okta 登录邮箱 |
| `CIQ_PASSWORD` | ✅ 必填 | 你的 Capital IQ 登录密码 |
| `CIQ_OUTPUT_DIR` | 可选 | 自定义下载目录（默认在 skill 目录下的 `output/`） |
| `CIQ_DEBUG` | 可选 | 设为 `true` 可开启有头浏览器模式，用于处理初次登录的 MFA 验证 |

---

## 📖 使用方法

配置完成后，你可以直接用自然语言指挥 Agent 执行复杂的金融数据检索任务。

### 示例对话

**搜索并下载：**
> “在 CIQ 上搜一下英伟达 (Nvidia) 最新的财报。”
> “帮我在 Capital IQ 上查找 Tesla 的研报，下载前 5 份。”
> “搜一下 Apple 最新的路演纪要 (Transcripts)。”

**查看结果：**
> “刚才下载的英伟达文件保存在哪了？”

Agent 会告知文件保存在 `output/公司名/` 目录下，格式涵盖 PDF 和 Excel。

---

## 📁 文件说明

```
ciq_financial_skill/
├── search_and_download.py   # 核心下载脚本（底层 API 直连 + 自动登录）
├── skill.json               # Skill 描述文件（Agent 调用的元数据）
├── SKILL.md                 # 详细的技术文档与排障指南
├── setup_env.sh             # 一键环境初始化脚本
├── requirements.txt         # Python 依赖清单
├── README.md                # 本文件
├── .env                     # 你的私密账号配置（Git 忽略）
├── ciq_cookies.json         # 登录状态缓存（Git 忽略，自动生成）
├── screenshots/             # 调试截图（仅在 DEBUG 模式生成）
└── output/                  # 最终下载的研报文档
```

---

## ❓ 常见问题

### 1. 遇到 MFA（手机验证码/扫码）验证怎么办？
由于 CIQ 属于企业级应用，通常强制开启多因素认证（MFA）。
如果初次登录失败，对 Agent 说：
> “开启 Capital IQ 的调试模式。”
Agent 会将 `CIQ_DEBUG` 设为 `true`。再次运行时会弹出可见的浏览器界面，此时你只需手动在网页上完成一次扫码或短信验证。成功后，登录状态会保存到 `ciq_cookies.json`，之后只要该缓存有效，就不再需要手动干预。

### 2. Cookie 过期了需要手动删掉吗？
不需要。脚本内置了 **Cookie 自动探针**。每次运行前会先验证缓存是否有效，若失效会自动触发 Playwright 重新登录流程。

### 3. 下载的文件能否直接打开？
可以。本 Skill 已经彻底解决了 CIQ 常见的“前端路由下载陷阱”（即下载成功但文件只有 54KB HTML 且无法打开的问题）。我们直接调用了 CIQ 底层的 `$value` 二进制流接口，确保下载的每一份都是完整的 PDF 或 Excel。

---

## 📜 许可
MIT License
