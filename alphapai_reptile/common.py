import os
import time
import random
import logging
from datetime import datetime
import pymysql
import requests
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()


class BaseCrawler:
    def __init__(self, log_dir):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'database': os.getenv('DB_NAME'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.setup_logging(log_dir)
        self.session = requests.Session()

    def setup_logging(self, log_dir):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()]
        )
        self.logger = logging.getLogger()

    def get_db_conn(self):
        return pymysql.connect(**self.db_config)

    def get_headers(self, token=None, keyword=None):
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "x-device": "a79b8f2eb003bea8be9af836fe0bf022",
            "x-from": "web",
            "Origin": "https://alphapai-web.rabyte.cn",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty"
        }
        if keyword:
            headers["referer"] = f"https://alphapai-web.rabyte.cn/reading/search?keyword={quote(keyword)}"
        if token:
            headers["authorization"] = token
        return headers

    def safe_request(self, method, url, **kwargs):
        try:
            time.sleep(random.uniform(1.0, 2.0))
            resp = self.session.request(method, url, timeout=15, **kwargs)
            if resp.status_code == 401: return "RELOGIN"
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            self.logger.error(f"请求出错 [{url}]: {str(e)}")
            return None

    def is_quiet_period(self):
        now = datetime.now().strftime("%H:%M")
        qs = os.getenv('QUIET_START', '00:01')
        qe = os.getenv('QUIET_END', '08:00')
        if qs <= now <= qe:
            self.logger.info(f"处于静默期 ({qs}-{qe})，跳过任务")
            return True
        return False

    def get_token(self):
        conn = self.get_db_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT token, expire_time FROM auth_credentials LIMIT 1")
                res = cursor.fetchone()
                if res and res['expire_time'] > datetime.now():
                    return res['token']

                self.logger.info("Token失效，尝试重新登录...")
                login_url = "https://alphapai-web.rabyte.cn/external/alpha/api/v2/authentication/accountLogin"
                payload = {"mobile": os.getenv('LOGIN_MOBILE'), "password": os.getenv('LOGIN_PASS')}
                r = self.safe_request("POST", login_url, json=payload, headers=self.get_headers())

                if r and r.get('code') == 200000:
                    token = r['data']['token']
                    # 修复此处：只有 2 个 %s，对应后面 2 个变量
                    sql = "REPLACE INTO auth_credentials (account, token, expire_time) VALUES (%s, %s, DATE_ADD(NOW(), INTERVAL 24 HOUR))"
                    cursor.execute(sql, (os.getenv('LOGIN_MOBILE'), token))
                    conn.commit()
                    self.logger.info("登录成功，Token已更新")
                    return token
                else:
                    self.logger.error(f"登录失败: {r}")
        finally:
            conn.close()
        return None