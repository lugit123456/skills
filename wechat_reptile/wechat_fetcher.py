import os
import pymysql
import json
import requests
import time
import random
import logging
from datetime import datetime as dt
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

# 仅获取 logger，配置在 scheduler_skill.py 中统一处理
logger = logging.getLogger('wechat_spider')

# --- 1. 统一配置加载 ---[cite: 3]
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_NAME'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4')
}

# 爬取链接后休眠配置[cite: 3]
LINK_SLEEP_TIME_MIN = float(os.getenv('LINK_SLEEP_TIME_MIN', 2))
LINK_SLEEP_TIME_MAX = float(os.getenv('LINK_SLEEP_TIME_MAX', 3))


def get_db_conn():
    return pymysql.connect(**DB_CONFIG)


# --- 2. 飞书通知与身份校验逻辑 ---[cite: 3]

def send_feishu_notification(message):
    """发送飞书机器人通知"""
    webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
    if not webhook_url:
        logger.warning("未配置 FEISHU_WEBHOOK_URL，跳过通知。")
        return

    payload = {
        "msg_type": "text",
        "content": {
            "text": f"⚠️ 【小龙虾系统告警】\n{message}"
        }
    }
    try:
        # 无论成功与否，不阻塞后续扫码逻辑[cite: 3]
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"飞书通知发送异常: {e}")


def verify_auth(token, cookie_str):
    """验证当前凭证是否有效"""
    test_url = "https://mp.weixin.qq.com/cgi-bin/appmsg"
    headers = {
        "Cookie": cookie_str,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    params = {
        "action": "list_ex", "token": token, "lang": "zh_CN",
        "f": "json", "ajax": "1", "begin": "0", "count": "1"
    }
    try:
        r = requests.get(test_url, params=params, headers=headers, timeout=10)
        # ret 为 0 表示登录态有效[cite: 3]
        return r.json().get("base_resp", {}).get("ret") == 0
    except:
        return False


def get_config_from_db():
    conn = get_db_conn()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT token, cookie_str FROM wechat_config WHERE id = 1")
            return cursor.fetchone()
    finally:
        conn.close()


def save_config_to_db(token, cookie_str):
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO wechat_config (id, token, cookie_str) VALUES (1, %s, %s) ON DUPLICATE KEY UPDATE token=%s, cookie_str=%s"
            cursor.execute(sql, (token, cookie_str, token, cookie_str))
        conn.commit()
    finally:
        conn.close()


def login_and_get_auth():
    """唤起有头模式浏览器扫码登录"""
    with sync_playwright() as p:
        # 强制开启有头模式以便客户扫码[cite: 3]
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        logger.info("请在弹出的浏览器中扫码登录微信公众号后台...")
        page.goto("https://mp.weixin.qq.com/")

        # 等待登录成功跳转，获取 Token[cite: 3]
        page.wait_for_url("**/cgi-bin/home?t=home/index&lang=zh_CN&token=*", timeout=0)
        new_token = page.url.split("token=")[1].split("&")[0]
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in context.cookies()])

        save_config_to_db(new_token, cookie_str)
        browser.close()
        return new_token, cookie_str


def get_valid_auth():
    """获取有效授权：失效时自动通知飞书并唤起扫码"""
    config = get_config_from_db()

    # 检查数据库中的凭证是否仍然有效[cite: 3]
    if config and config['token'] and config['cookie_str']:
        if verify_auth(config['token'], config['cookie_str']):
            return config['token'], config['cookie_str']
        else:
            # 凭证失效，发送飞书消息[cite: 3]
            logger.warning("凭证失效，正在发送飞书通知并唤起扫码...")
            send_feishu_notification("微信公众号登录已过期！请立即在服务器/本地操作扫码登录。")

    # 无论通知是否成功，直接进入扫码逻辑[cite: 3]
    return login_and_get_auth()


# --- 3. 核心抓取生成器 ---[cite: 3]

def fetch_batch_articles(fakeid, nickname, token, cookie):
    conn = get_db_conn()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    cursor.execute("SELECT last_begin FROM bloggers WHERE fakeid = %s", (fakeid,))
    row = cursor.fetchone()
    begin = row['last_begin'] if row else 0
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }

    try:
        while True:
            current_page = begin // 5 + 1
            logger.info(f"开始爬取 [{nickname}] 第 {current_page} 页，begin={begin}")

            params = {
                "sub": "list", "begin": str(begin), "count": "5", "fakeid": fakeid,
                "type": "101_1", "free_publish_type": "1", "sub_action": "list_ex",
                "token": token, "lang": "zh_CN", "f": "json", "ajax": "1"
            }
            res = requests.get("https://mp.weixin.qq.com/cgi-bin/appmsgpublish", params=params, headers=headers)
            data = res.json()

            if data.get("base_resp", {}).get("ret") != 0:
                yield {"status": "error", "msg": data.get("base_resp")}
                return

            publish_page = json.loads(data.get("publish_page", "{}"))
            total_count = int(publish_page.get("total_count", 0))
            publish_list = publish_page.get("publish_list", [])

            # 判定 1: 到底了[cite: 3]
            if not publish_list:
                logger.info(f"[{nickname}] 列表为空，历史爬取完成。")
                cursor.execute("UPDATE bloggers SET is_finished = 1, total_count = %s WHERE fakeid = %s",
                               (total_count, fakeid))
                conn.commit()
                yield {"status": "finished"}
                return

            inserted_count = 0
            for item in publish_list:
                # 兼容老数据结构[cite: 3]
                info = json.loads(item["publish_info"]) if "publish_info" in item and item["publish_info"] else item

                for article in info.get("appmsgex", []):
                    sn = article.get("aid")
                    dt_str = dt.fromtimestamp(article["create_time"]).strftime('%Y-%m-%d %H:%M:%S')

                    cursor.execute(
                        "INSERT IGNORE INTO articles (fakeid, article_sn, title, link, create_time) VALUES (%s, %s, %s, %s, %s)",
                        (fakeid, sn, article["title"], article["link"], dt_str)
                    )
                    inserted_count += cursor.rowcount  # 统计实际插入成功的条数[cite: 3]

            # 判定 2: 增量判定[cite: 3]
            if inserted_count == 0:
                logger.info(f"[{nickname}] 发现重复页，触发增量跳过。")
                cursor.execute("UPDATE bloggers SET is_finished = 1, total_count = %s WHERE fakeid = %s",
                               (total_count, fakeid))
                conn.commit()
                yield {"status": "exists"}
                return

            begin += 5
            cursor.execute("UPDATE bloggers SET last_begin = %s, total_count = %s WHERE fakeid = %s",
                           (begin, total_count, fakeid))
            conn.commit()

            sleep_time = random.uniform(LINK_SLEEP_TIME_MIN, LINK_SLEEP_TIME_MAX)
            logger.info(f"第 {current_page} 页处理完成，休眠 {sleep_time:.2f} 秒")
            time.sleep(sleep_time)
            yield {"status": "running", "begin": begin}
    finally:
        conn.close()