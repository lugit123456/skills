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
            self.logger.info(f"[*] 当前处于静默避风港期 ({qs} - {qe})，强制跳过任务")
            return True
        return False

    def apply_start_delay(self, min_env, max_env):
        """执行启动前的随机休眠"""
        delay_min = int(os.getenv(min_env, 0))
        delay_max = int(os.getenv(max_env, 0))
        if delay_max > 0:
            sleep_time = random.uniform(delay_min, delay_max)
            self.logger.info(f"[*] 任务启动前随机休眠: {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)

    def get_token(self):
        conn = self.get_db_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT token, expire_time FROM auth_credentials LIMIT 1")
                res = cursor.fetchone()
                if res and res['expire_time'] > datetime.now():
                    return res['token']

                self.logger.warning("[!] Token 已失效或不存在，小龙虾尝试调用模拟登录接口...")
                login_url = "https://alphapai-web.rabyte.cn/external/alpha/api/v2/authentication/accountLogin"
                payload = {"mobile": os.getenv('LOGIN_MOBILE'), "password": os.getenv('LOGIN_PASS')}
                r = self.safe_request("POST", login_url, json=payload, headers=self.get_headers())

                if r and r.get('code') == 200000:
                    token = r['data']['token']
                    sql = "REPLACE INTO auth_credentials (account, token, expire_time) VALUES (%s, %s, DATE_ADD(NOW(), INTERVAL 24 HOUR))"
                    cursor.execute(sql, (os.getenv('LOGIN_MOBILE'), token))
                    conn.commit()
                    self.logger.info("[√] 登录成功，Token 已自动刷新并入库。")
                    return token
                else:
                    self.logger.error(f"[X] 自动登录失败，请检查密码或网络。返回值: {r}")
        finally:
            conn.close()
        return None