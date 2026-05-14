import os
import re
import json
import time
import argparse
import traceback
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

# 环境变量配置
OUTPUT_DIR = os.getenv("CIQ_OUTPUT_DIR", os.path.join(PROJECT_DIR, "output"))
CIQ_USERNAME = os.getenv("CIQ_USERNAME")
CIQ_PASSWORD = os.getenv("CIQ_PASSWORD")
DEBUG = os.getenv("CIQ_DEBUG", "false").lower() in ("true", "1", "yes")

COOKIE_FILE = os.path.join(PROJECT_DIR, "ciq_cookies.json")
SCREENSHOT_DIR = os.path.join(PROJECT_DIR, "screenshots")

# 接口常量
SEARCH_API_URL = "https://www.capitaliq.spglobal.com/apisv3/search-service/v3/OmniSearch/VerticalSearch"
DOWNLOAD_API_URL_TEMPLATE = "https://www.capitaliq.spglobal.com/SNL.Services.Data.Api.Service/v2/Internal/General/DocsFileVersions({attachment_id})/$value"

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "com-gmi-application-id": "spg-webplatform-core",
    "com-gmi-call-name": "/apisv3/spg-webplatform-core/search/searchResults",
    "content-type": "application/json",
    "origin": "https://www.capitaliq.spglobal.com",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
}


def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if DEBUG:
        os.makedirs(SCREENSHOT_DIR, exist_ok=True)


def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '_', filename)


def load_cached_cookies():
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
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
    if not cookie_dict:
        return False

    print(f"[auth] Verifying cached cookies...")
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
    probe_headers = BASE_HEADERS.copy()
    probe_headers["cookie"] = cookie_str

    probe_payload = {
        "pageSize": 1,
        "skipCaches": False,
        "pageStart": 0,
        "verticalId": "financials_research-gss",
        "rawSearchRequest": {"query": "test", "searchFilters": []},
        "sort": {"key": "", "dir": "desc"}
    }

    try:
        with requests.Session() as s:
            res = s.post(SEARCH_API_URL, headers=probe_headers, json=probe_payload, timeout=10)
            if res.status_code == 200 and "searchHits" in res.json():
                print("[auth] ✓ Cookies are valid and active!")
                return True
    except Exception:
        pass

    print("[auth] ✗ Cookies expired or invalid. Re-authentication required.")
    return False


def get_fresh_cookies():
    print("[login] Launching Playwright for SSO login...")
    # DEBUG 模式开启时显示浏览器界面，方便处理 Okta 扫码/短信等 MFA
    headless_mode = not DEBUG

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless_mode)
        context = browser.new_context(user_agent=BASE_HEADERS['user-agent'])
        page = context.new_page()

        try:
            page.goto("https://www.capitaliq.spglobal.com/", timeout=60000)
            print("[login] Entering credentials...")

            # 邮箱
            page.wait_for_selector("input[name='identifier'], input[type='email']", timeout=15000)
            page.fill("input[name='identifier'], input[type='email']", CIQ_USERNAME)
            page.click("input[type='submit'], button[type='submit']")

            # 密码
            page.wait_for_selector("input[name='credentials.passcode'], input[type='password']", timeout=15000)
            page.fill("input[name='credentials.passcode'], input[type='password']", CIQ_PASSWORD)
            page.click("input[type='submit'], button[type='submit']")

            print("[login] Waiting for SSO redirect to CIQ dashboard...")
            print("[login] (If stuck here, set CIQ_DEBUG=true in .env to check for MFA prompts)")

            page.wait_for_url("**/apisv3/cpd-dashboard/**", timeout=60000)
            page.wait_for_timeout(3000)  # Ensure tokens are set

            cookies = context.cookies()
            cookie_dict = {c['name']: c['value'] for c in cookies}
            save_cookies(cookie_dict)
            return cookie_dict

        except PlaywrightTimeoutError:
            print("[login] ✗ Timeout during login. Check for MFA/Captchas.")
            if DEBUG:
                page.screenshot(path=os.path.join(SCREENSHOT_DIR, "login_timeout.png"))
            return None
        except Exception as e:
            print(f"[login] ✗ Unexpected error: {e}")
            return None
        finally:
            browser.close()


def fetch_search_results(session, company_name, active_headers, limit):
    payload = {
        "pageSize": limit,
        "skipCaches": False,
        "pageStart": 0,
        "verticalId": "financials_research-gss",
        "rawSearchRequest": {"query": company_name, "searchFilters": []},
        "sort": {"key": "", "dir": "desc"}
    }

    print(f"\n[search] Querying CIQ for: '{company_name}' (Limit: {limit})")
    response = session.post(SEARCH_API_URL, headers=active_headers, json=payload)

    if response.status_code != 200:
        print(f"[search] ✗ API Error {response.status_code}: {response.text[:200]}")
        return []

    hits = response.json().get("searchHits", [])
    print(f"[search] ✓ Found {len(hits)} matching documents.")
    return hits


def download_file(session, attachment_id, save_path, active_headers):
    download_url = DOWNLOAD_API_URL_TEMPLATE.format(attachment_id=attachment_id)
    try:
        response = session.get(download_url, headers=active_headers, stream=True)
        response.raise_for_status()

        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"  [+] Downloaded -> {os.path.basename(save_path)}")
        return True
    except Exception as e:
        print(f"  [-] Failed to download: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Capital IQ Financial Reports Downloader")
    parser.add_argument("company", help="Target company or keyword (e.g., 'Apple')")
    parser.add_argument("-n", "--count", type=int, default=10, help="Number of results to download")
    args = parser.parse_args()

    company_name = args.company
    download_count = args.count

    if not CIQ_USERNAME or not CIQ_PASSWORD:
        print("[error] CIQ_USERNAME and CIQ_PASSWORD must be set in .env")
        sys.exit(1)

    ensure_dirs()
    target_dir = os.path.join(OUTPUT_DIR, sanitize_filename(company_name))
    os.makedirs(target_dir, exist_ok=True)

    # 1. Auth Flow
    cookies = load_cached_cookies()
    if not check_cookie_validity(cookies):
        cookies = get_fresh_cookies()
        if not cookies:
            print("[error] Cannot proceed without valid authentication.")
            sys.exit(1)

    # Convert to raw header string to bypass requests CookieJar domain issues
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
    active_headers = BASE_HEADERS.copy()
    active_headers["cookie"] = cookie_str

    # 2. Search & Download
    downloaded = 0
    skipped = 0

    with requests.Session() as session:
        hits = fetch_search_results(session, company_name, active_headers, download_count)

        for idx, hit in enumerate(hits[:download_count]):
            display_data = hit.get("displayData", {})
            report_info = display_data.get("Report", {})
            attachments = display_data.get("Attachments", [])

            raw_title = report_info.get("text", f"Unnamed_Report_{idx}")
            safe_title = sanitize_filename(raw_title)

            for att in attachments:
                file_type = att.get("filetype", "").upper()
                att_id = att.get("attachmentId")

                if file_type in ["PDF", "XLSX", "XLS"]:
                    ext = file_type.lower()
                    file_name = f"{safe_title}.{ext}"
                    save_path = os.path.join(target_dir, file_name)

                    if os.path.exists(save_path):
                        print(f"  [⏭] Skipped (exists) -> {file_name}")
                        skipped += 1
                        continue

                    if download_file(session, att_id, save_path, active_headers):
                        downloaded += 1
                        time.sleep(1)  # Polite delay

    print(f"\n{'=' * 50}")
    print(f"Task Complete for: {company_name}")
    print(f"Downloaded: {downloaded} | Skipped: {skipped}")
    print(f"Location: {target_dir}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()