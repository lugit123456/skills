import os
import re
import json
import time
import argparse
import requests
import random
import urllib.parse
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

# ================= 环境变量与配置 =================
OUTPUT_DIR = os.getenv("CIQ_OUTPUT_DIR", os.path.join(PROJECT_DIR, "output"))
CIQ_USERNAME = os.getenv("CIQ_USERNAME")
CIQ_PASSWORD = os.getenv("CIQ_PASSWORD")

COOKIE_FILE = os.path.join(PROJECT_DIR, "ciq_cookies.json")

# ================= 接口常量 =================
REPORT_SEARCH_API = "https://www.capitaliq.spglobal.com/apisv3/search-service/v3/OmniSearch/VerticalSearch"
COMPANY_SEARCH_API = "https://www.capitaliq.spglobal.com/apisv3/search-service/v3/OmniSearch/OmniSearch"
MENU_API_TEMPLATE = "https://www.capitaliq.spglobal.com/apisv3/menu-service/v1/LeftNavigationMenu/0?keyentitytype=8&keyEntity={hit_id}"

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "com-gmi-application-id": "spg-webplatform-core",
    "content-type": "application/json",
    "origin": "https://www.capitaliq.spglobal.com",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
}


# ================= 基础工具函数 =================
def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '_', filename)


def load_cached_cookies():
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None


def save_cookies(cookie_dict):
    try:
        with open(COOKIE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookie_dict, f)
        print(f"[auth] Cookie cached to {COOKIE_FILE}")
    except Exception as e:
        print(f"[auth] Failed to save cookie: {e}")


def check_cookie_validity(cookie_dict):
    if not cookie_dict: return False
    print(f"[auth] Verifying cached cookies...")
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
    probe_headers = BASE_HEADERS.copy()
    probe_headers["cookie"] = cookie_str

    probe_payload = {
        "pageSize": 1, "skipCaches": False, "pageStart": 0,
        "verticalId": "financials_research-gss",
        "rawSearchRequest": {"query": "test", "searchFilters": []},
        "sort": {"key": "", "dir": "desc"}
    }
    try:
        with requests.Session() as s:
            res = s.post(REPORT_SEARCH_API, headers=probe_headers, json=probe_payload, timeout=10)
            if res.status_code == 200 and "searchHits" in res.json():
                print("[auth] ✓ Cookies are valid and active!")
                return True
    except:
        pass
    print("[auth] ✗ Cookies expired or invalid. Re-authentication required.")
    return False


def get_fresh_cookies(is_headless):
    print("[login] 正在启动浏览器进行 SSO 登录...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=is_headless)
        context = browser.new_context(user_agent=BASE_HEADERS['user-agent'])
        page = context.new_page()
        try:
            page.goto("https://www.capitaliq.spglobal.com/", timeout=60000)
            print("[login] 输入账号密码...")
            page.wait_for_selector("input[name='identifier'], input[type='email']", timeout=15000)
            page.fill("input[name='identifier'], input[type='email']", CIQ_USERNAME)
            page.click("input[type='submit'], button[type='submit']")

            page.wait_for_selector("input[name='credentials.passcode'], input[type='password']", timeout=15000)
            page.fill("input[name='credentials.passcode'], input[type='password']", CIQ_PASSWORD)
            page.click("input[type='submit'], button[type='submit']")

            print("[login] 等待系统重定向到主页...")
            page.wait_for_url("https://www.capitaliq.spglobal.com/apisv3/spg-webplatform-core/news/home", timeout=60000)
            page.wait_for_timeout(3000)

            cookies = context.cookies()
            cookie_dict = {c['name']: c['value'] for c in cookies}
            save_cookies(cookie_dict)
            return cookie_dict
        except PlaywrightTimeoutError:
            print("[login] ✗ 登录超时。请加上 --headed 参数检查是否需要验证码。")
            return None
        finally:
            browser.close()


# ================= 核心：单页任务处理执行器 =================
def execute_single_download(page, item, target_dir, file_ext, btn_text):
    """
    独立封装的下载器，返回布尔值以支持失败重试机制
    """
    title = item["title"]
    ui_url = item["url"]
    save_path = os.path.abspath(os.path.join(target_dir, f"[自动化导出]_{title}{file_ext}"))

    if os.path.exists(save_path):
        print(f"  [⏭] 文件已存在，跳过下载: {save_path}")
        return True

    try:
        # 修复点 1: wait_until="domcontentloaded" 避免死等广告追踪器，同时将超时拉长到 60 秒
        page.goto(ui_url, wait_until="domcontentloaded", timeout=60000)

        max_wait_loops = 15
        menu_btn = None
        target_frame = None

        for loop_idx in range(max_wait_loops):
            frames_to_search = [page] + page.frames
            for frame in frames_to_search:
                btn = frame.locator(
                    'a.snl-three-dots-icon, a[title="Page Tools"], a.dropdown-toggle:has-text("导出")').first
                if btn.count() > 0:
                    menu_btn = btn
                    target_frame = frame
                    break
            if menu_btn is not None: break
            page.wait_for_timeout(2000)

        if menu_btn is None:
            print(f"  -> ❌ 等待超时：页面未能完全渲染。")
            return False

        page.wait_for_timeout(1000)
        menu_btn.evaluate("node => node.click()")
        page.wait_for_timeout(1500)

        export_btn = target_frame.locator(f'a.hui-toolbutton:has-text("{btn_text}"), a:has-text("{btn_text}")').first
        if export_btn.count() > 0:
            print(f"  -> 📥 找到【{btn_text}】选项，触发下载...")
            with page.expect_download(timeout=60000) as download_info:
                export_btn.evaluate("node => node.click()")

            download = download_info.value
            download.save_as(save_path)
            print(f"  -> ✅ 物理下载成功，文件保存至: {save_path}")
            return True
        else:
            print(f"  -> ❌ 未能在菜单中找到【{btn_text}】选项，可能当前报表不支持。")
            return False

    except Exception as e:
        print(f"  -> ❌ 处理异常: {e}")
        return False


# ================= 任务二：提取与调度中心 =================
def task_fetch_financial_json(session, company_name, active_headers, base_output_dir, file_format, is_headless):
    print(f"\n{'=' * 50}\n[Task 2] 正在提取【{company_name}】的相关报表数据...\n{'=' * 50}")

    target_dir = os.path.join(base_output_dir, sanitize_filename(company_name), file_format)
    os.makedirs(target_dir, exist_ok=True)

    file_ext = {"excel": ".xlsx", "pdf": ".pdf", "word": ".docx"}.get(file_format, ".xlsx")
    btn_text = {"excel": "导出到Excel为数据", "pdf": "导出为PDF", "word": "导出到Word"}.get(file_format,
                                                                                            "导出到Excel为数据")

    # 获取 hitId
    search_payload = {
        "verticalIds": [], "pageSize": 5, "skipCaches": False,
        "rawSearchRequest": {"query": company_name, "searchFilters": []}
    }
    search_res = session.post(COMPANY_SEARCH_API, headers=active_headers, json=search_payload)
    if search_res.status_code != 200: return
    try:
        hit_id = search_res.json()['verticalResponses'][0]['searchHits'][0]['hitId']
    except:
        return

    # 获取菜单树
    menu_url = MENU_API_TEMPLATE.format(hit_id=hit_id)
    menu_res = session.get(menu_url, headers=active_headers)
    if menu_res.status_code != 200: return

    menu_data = menu_res.json()
    target_api_nodes = []

    # 递归提取 API (修复了“业绩指引”等非标准报表的提取问题)
    def extract_table_apis(nodes):
        for node in nodes:
            title = node.get("Title", "Unknown")
            raw_url = node.get("Url", "")

            parsed_url = urllib.parse.urlparse(raw_url.replace("#", "http://dummy/"))
            qs = urllib.parse.parse_qs(parsed_url.query)

            # 正常报表有 keypage
            keypage = qs.get("keypage", [None])[0] or node.get("HydraKeyPage") or node.get("KeyPage")

            # 修复点 2: 优先处理带有实际独立链接的特殊模块（如业绩指引 / 趋势等）
            if title in ["业绩指引", "一致预测", "详细预估", "意外", "趋势", "修订"] and raw_url:
                ui_url = f"https://www.capitaliq.spglobal.com{raw_url}"
                target_api_nodes.append({"title": sanitize_filename(title), "url": ui_url})
            elif keypage:
                ui_url = f"https://www.capitaliq.spglobal.com/apisv3/spg-webplatform-core/company/report?id={hit_id}&keypage={keypage}"
                target_api_nodes.append({"title": sanitize_filename(title), "url": ui_url})

            if "Children" in node and node["Children"]:
                extract_table_apis(node["Children"])

    for item in menu_data.get("NavigationLeftItems", []):
        menu_title = item.get("Title", "")
        if item.get("Id") == 2 or menu_title == "财务数据":
            extract_table_apis(item.get("Children", []))
        elif menu_title == "预测数据":
            extract_table_apis(item.get("Children", []))  # 直接把预测数据下的都抓了

    print(f"[*] 菜单解析完毕，精准定位到 {len(target_api_nodes)} 个相关报表。")

    # ================= UI 下载启动引擎 =================
    cookie_str = active_headers.get("cookie", "")
    pw_cookies = [{"name": k, "value": v, "domain": ".capitaliq.spglobal.com", "path": "/"} for k, v in
                  [pair.split("=", 1) for pair in cookie_str.split("; ") if "=" in pair]]

    failed_tasks = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=is_headless)
        context = browser.new_context(accept_downloads=True, user_agent=BASE_HEADERS['user-agent'])
        context.add_cookies(pw_cookies)
        page = context.new_page()

        # 第一轮：全量下载
        for idx, item in enumerate(target_api_nodes):
            print(f"🔄 [初轮 {idx + 1}/{len(target_api_nodes)}] 正在访问并渲染页面: {item['title']}")
            success = execute_single_download(page, item, target_dir, file_ext, btn_text)

            if not success:
                failed_tasks.append(item)

            if idx < len(target_api_nodes) - 1:
                sleep_time = random.uniform(3, 7)
                page.wait_for_timeout(sleep_time * 1000)

        # 修复点 3：失败重试机制（如果第一轮全部成功，此区块将被跳过）
        if failed_tasks:
            print(f"\n⚠️ 发现 {len(failed_tasks)} 个任务下载失败或超时，开始最终重试...\n")
            page.wait_for_timeout(5000)  # 重试前给服务器和本地网络一点缓冲时间

            for idx, item in enumerate(failed_tasks):
                print(f"🔄 [重试 {idx + 1}/{len(failed_tasks)}] 再次尝试渲染页面: {item['title']}")

                # 重试时如果还失败就直接放弃，防止陷入死循环
                success = execute_single_download(page, item, target_dir, file_ext, btn_text)

                if idx < len(failed_tasks) - 1:
                    sleep_time = random.uniform(4, 8)
                    page.wait_for_timeout(sleep_time * 1000)

        browser.close()


# ================= 主入口 =================
def main():
    parser = argparse.ArgumentParser(description="Capital IQ 投研自动化套件")
    parser.add_argument("company", help="目标公司关键词 (例如: 'Nvidia')")
    parser.add_argument("-f", "--format", choices=["excel", "pdf", "word"], default="pdf", help="下载文件的格式")
    parser.add_argument("--headed", action="store_true", help="显示浏览器界面运行")
    args = parser.parse_args()

    company_name = args.company
    file_format = args.format
    is_headless = not args.headed

    if not CIQ_USERNAME or not CIQ_PASSWORD:
        print("[error] 请确保配置了账号密码")
        import sys;
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cookies = load_cached_cookies()
    if not check_cookie_validity(cookies):
        cookies = get_fresh_cookies(is_headless)
        if not cookies:
            print("[error] 登录失败。")
            import sys;
            sys.exit(1)

    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
    active_headers = BASE_HEADERS.copy()
    active_headers["cookie"] = cookie_str

    with requests.Session() as session:
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504],
                               allowed_methods=["HEAD", "GET", "OPTIONS", "POST"])
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)

        task_fetch_financial_json(session, company_name, active_headers, OUTPUT_DIR, file_format, is_headless)

    print(f"\n{'=' * 50}\n全自动流程执行完毕！\n{'=' * 50}")


if __name__ == "__main__":
    main()