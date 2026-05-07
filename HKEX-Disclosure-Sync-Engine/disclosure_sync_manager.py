import os
import time
import random
import logging
from logging.handlers import TimedRotatingFileHandler
import hashlib
from io import StringIO
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
import mysql.connector
from dotenv import load_dotenv
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context

# 1. 路径与环境配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


# 2. 强化日志配置 (解决日志没记录及 7 天保存问题)
def setup_logging():
    log_dir = os.path.join(BASE_DIR, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 核心修改：在初始化文件名时就加上日期
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f'disclosure_sync_{today_str}.log')

    # 依然使用 TimedRotatingFileHandler 以便自动清理 7 天前的日志
    # 注意：因为文件名已经带了日期，when="midnight" 触发时会产生双重日期后缀，
    # 但由于它能自动处理 backupCount，这仍是实现“保留7天”最简单的办法。
    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=7, encoding='utf-8'
    )

    # 控制台输出
    console_handler = logging.StreamHandler()

    # 日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


setup_logging()


class TlsAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.load_default_certs()
        kwargs['ssl_context'] = context
        return super(TlsAdapter, self).init_poolmanager(*args, **kwargs)


class OmniHKEXCrawler:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4'
        }
        self.base_url = "https://di.hkex.com.hk/di/NSAllFormDateList.aspx"
        self.session = requests.Session()
        self.session.mount("https://", TlsAdapter())
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": self.base_url
        }
        self.session_end_time = None
        self.state = {}
        self.is_caught_up = False

    def get_conn(self):
        return mysql.connector.connect(**self.db_config)

    def _generate_fingerprint_v12(self, r):
        seed = f"{r['serial']}{r['date']}{r['corp']}{r['person']}{r['reason']}{r['shares']}{r['price']}{r['total']}{r['percent']}{r['assoc_url']}"
        return hashlib.md5(seed.encode('utf-8')).hexdigest()

    def _format_date(self, d_str):
        if not d_str or d_str == "nan" or d_str.strip() == "": return None
        try:
            return datetime.strptime(d_str.strip(), '%d/%m/%Y').strftime('%Y-%m-%d')
        except:
            return None

    def load_state(self):
        conn = self.get_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM crawler_status WHERE id = 1")
        self.state = cursor.fetchone()
        cursor.close()
        conn.close()

    def update_state(self, win_start, win_end, page, mode=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        if mode:
            sql = "UPDATE crawler_status SET current_window_start=%s, current_window_end=%s, current_page=%s, last_mode=%s WHERE id=1"
            cursor.execute(sql, (win_start, win_end, page, mode))
        else:
            sql = "UPDATE crawler_status SET current_window_start=%s, current_window_end=%s, current_page=%s WHERE id=1"
            cursor.execute(sql, (win_start, win_end, page))
        conn.commit()
        cursor.close()
        conn.close()

    def acquire_lock(self):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT is_running FROM crawler_status WHERE id = 1 FOR UPDATE")
        if cursor.fetchone()[0] == 1: return False
        cursor.execute("UPDATE crawler_status SET is_running = 1 WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()
        return True

    def release_lock(self):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE crawler_status SET is_running = 0 WHERE id = 1")
        conn.commit()
        cursor.close()
        conn.close()

    def run(self):
        if not self.acquire_lock():
            logging.warning("原子锁占用，爬虫任务跳过。")
            return

        try:
            self.load_state()
            current_mode = self.state['last_mode']
            lifespan = random.randint(int(os.getenv('SESSION_LIFESPAN_MIN', 300)),
                                      int(os.getenv('SESSION_LIFESPAN_MAX', 900)))
            self.session_end_time = datetime.now() + timedelta(seconds=lifespan)
            logging.info(f"引擎启动 | 模式: {current_mode} | 任务寿命: {lifespan}s")

            if current_mode == 'INCREMENTAL':
                win_end = datetime.now().date()
                win_start = win_end - timedelta(days=2)
                self._main_loop(win_start, win_end, 1, is_bulk=False)
            else:
                self._main_loop(self.state['current_window_start'], self.state['current_window_end'],
                                self.state['current_page'], is_bulk=True)
        finally:
            self.release_lock()

    def _main_loop(self, win_start, win_end, page, is_bulk):
        global_target_end = self.state['global_target_end']
        while win_start <= (global_target_end if is_bulk else datetime.now().date()):
            while True:
                if datetime.now() > self.session_end_time:
                    logging.info(f"寿命耗尽，优雅退出。当前页码: {page}")
                    if is_bulk: self.update_state(win_start, win_end, page)
                    return

                page_start = time.time()
                is_empty, count = self.process_page(win_start, win_end, page, is_bulk)
                duration = time.time() - page_start

                if not is_bulk and self.is_caught_up:
                    logging.info("增量同步已追平历史记录。")
                    return

                if is_empty:
                    if not is_bulk: return
                    if win_end >= global_target_end:
                        logging.info("全量补课完成，切换为增量模式。")
                        self.update_state(win_start, win_end, 1, mode='INCREMENTAL')
                        return
                    win_start = win_end + timedelta(days=1)
                    win_end = min(win_start + timedelta(days=180), global_target_end)
                    page = 1
                    self.update_state(win_start, win_end, page)
                    break
                else:
                    sleep_t = random.uniform(float(os.getenv('ITEM_SLEEP_MIN', 2)),
                                             float(os.getenv('ITEM_SLEEP_MAX', 5)))
                    logging.info(f"Page {page} | 入库: {count}条 | 耗时: {duration:.2f}s | 休眠: {sleep_t:.1f}s")
                    page += 1
                    if is_bulk: self.update_state(win_start, win_end, page)
                    time.sleep(sleep_t)

    def process_page(self, sd, ed, pn, is_bulk):
        s_str, e_str = sd.strftime('%d/%m/%Y'), ed.strftime('%d/%m/%Y')
        params = {'sa1': 'da', 'scsd': s_str, 'sced': e_str, 'src': 'MAIN', 'lang': 'ZH', 'pg': pn}
        try:
            resp = self.session.get(self.base_url, params=params, headers=self.headers, timeout=30)
            df_list = pd.read_html(StringIO(resp.text), attrs={'id': 'grdPaging'}, header=0)
            if not df_list or len(df_list[0]) == 0: return True, 0

            df = df_list[0]
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows_html = soup.find('table', id='grdPaging').find_all('tr')[1:]

            d_links, a_links = [], []
            for tr in rows_html:
                tds = tr.find_all('td')
                if not tds: continue
                a_m = tds[0].find('a') or tds[1].find('a')
                d_links.append(urljoin(self.base_url, a_m['href']) if a_m else "")
                a_a = tds[9].find('a')
                a_links.append(urljoin(self.base_url, a_a['href']) if a_a else "")

            saved = self._save_data(df, d_links, a_links, is_bulk)
            return False, saved
        except Exception as e:
            logging.error(f"处理第 {pn} 页报错: {e}")
            return False, 0

    def _save_data(self, df, d_links, a_links, is_bulk):
        conn = self.get_conn()
        cursor = conn.cursor()
        saved = 0
        for i, (_, row) in enumerate(df.iterrows()):
            def c(v):
                return str(v).strip() if pd.notna(v) and str(v) != 'nan' else ""

            r_info = {'serial': c(row.iloc[0]), 'date': c(row.iloc[1]), 'corp': c(row.iloc[2]),
                      'person': c(row.iloc[3]), 'reason': c(row.iloc[4]), 'shares': c(row.iloc[5]),
                      'price': c(row.iloc[6]), 'total': c(row.iloc[7]), 'percent': c(row.iloc[8]),
                      'assoc_txt': c(row.iloc[9]), 'deb': c(row.iloc[10]), 'main_url': d_links[i],
                      'assoc_url': a_links[i]}

            if not is_bulk and r_info['serial']:
                cursor.execute("SELECT 1 FROM hkex_disclosures WHERE form_serial_no = %s", (r_info['serial'],))
                if cursor.fetchone():
                    self.is_caught_up = True
                    break

            fp = self._generate_fingerprint_v12(r_info)
            fmt_date = self._format_date(r_info['date'])
            sql = """INSERT IGNORE INTO hkex_disclosures (form_serial_no, fingerprint, detail_url, assoc_corp_url, event_date, event_date_formatted, corp_name, person_name, reason_text, shares_involved, avg_price, total_shares, share_percentage, assoc_corp_interests, debenture_interests) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            params = (r_info['serial'], fp, r_info['main_url'], r_info['assoc_url'], r_info['date'], fmt_date,
                      r_info['corp'], r_info['person'], r_info['reason'], r_info['shares'], r_info['price'],
                      r_info['total'], r_info['percent'], r_info['assoc_txt'], r_info['deb'])
            cursor.execute(sql, params)
            if cursor.rowcount > 0: saved += 1
        conn.commit()
        cursor.close()
        conn.close()
        return saved


if __name__ == "__main__":
    crawler = OmniHKEXCrawler()
    crawler.run()