import os
import re
import time
import json
import random
import string
import logging
import pymysql
import requests
import html2text
from datetime import datetime, timedelta, time as dt_time
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# 获取 .env 所在目录作为基准路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


# --- 1. 配置加载与路径初始化 ---
def get_path(env_key, default):
    rel_path = os.getenv(env_key, default)
    full_path = os.path.join(BASE_DIR, rel_path)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    return full_path


LOG_ROOT = get_path('REPTILE_DETAILS_LOG_DIR', 'logs/reptile_details')
DATA_ROOT = get_path('DATA_ROOT', 'wechat_detail')
# 自动创建子目录
MD_DIR = os.path.join(DATA_ROOT, 'markdowns')
IMG_DIR = os.path.join(DATA_ROOT, 'images')
for d in [MD_DIR, IMG_DIR]:
    if not os.path.exists(d): os.makedirs(d)

# --- 2. 日志系统配置 ---
log_file = os.path.join(LOG_ROOT, f"detail_{datetime.now().strftime('%Y%m%d')}.log")
logger = logging.getLogger('wechat_detail')
logger.setLevel(logging.INFO)

if logger.handlers: logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler(log_file, encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


# --- 3. 详情抓取执行器 ---
class WeChatDetailCrawler:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'db': os.getenv('DB_NAME'),
            'charset': os.getenv('DB_CHARSET', 'utf8mb4')
        }
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.body_width = 0  # 不换行，保持长句
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }

    def _get_conn(self):
        return pymysql.connect(**self.db_config)

    def _get_env_time(self, key, default):
        time_str = os.getenv(key, default)
        try:
            h, m = map(int, time_str.split(':'))
            return dt_time(h, m)
        except:
            dh, dm = map(int, default.split(':'))
            return dt_time(dh, dm)

    def _is_quiet_time(self):
        now = datetime.now().time()
        q_start = self._get_env_time("QUIET_START", "00:00")
        q_end = self._get_env_time("QUIET_END", "00:00")
        if q_start > q_end:
            return now >= q_start or now < q_end
        return q_start <= now < q_end

    def _set_lock(self, status):
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("UPDATE wechat_config SET is_detail_running = %s WHERE id = 1", (status,))
        conn.commit()
        conn.close()

    def _check_lock(self):
        conn = self._get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT is_detail_running FROM wechat_config WHERE id = 1")
            res = cur.fetchone()
        conn.close()
        return res['is_detail_running'] if res else 0

    def _get_pending_tasks(self, batch_size=15):
        conn = self._get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, fakeid, article_sn, title, link, create_time FROM articles WHERE crawl_status = 0 LIMIT %s",
                (batch_size,))
            tasks = cur.fetchall()
        conn.close()
        return tasks

    def _download_img(self, url, date_str, fakeid, sn):
        """下载图片，文件名剔除特殊字符以兼容预览"""
        try:
            curr_img_dir = os.path.join(IMG_DIR, date_str)
            if not os.path.exists(curr_img_dir): os.makedirs(curr_img_dir)

            # 剔除 fakeid 中的 '=' 避免 VS Code 预览识别异常
            safe_fakeid = fakeid.replace('=', '')
            random_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            img_name = f"{safe_fakeid}_{sn}_{random_code}.jpg"
            img_path = os.path.join(curr_img_dir, img_name)

            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                with open(img_path, 'wb') as f: f.write(r.content)
                # 计算从 MD 文件夹到图片文件夹的相对路径
                return f"../images/{date_str}/{img_name}"
        except Exception as e:
            logger.error(f"图片下载失败: {url} - {e}")
        return url

    def run(self):
        # 静默期判定
        if self._is_quiet_time():
            msg = f"🌙 当前处于静默期 ({os.getenv('QUIET_START')})，触发器放弃任务并进入休眠。"
            logger.info(msg)
            return msg

        if self._check_lock() == 1:
            logger.warning("旧详情任务还未结束，新任务自动关闭。")
            return

        wait_start = random.uniform(int(os.getenv('DETAIL_START_DELAY_MIN', 5)),
                                    int(os.getenv('DETAIL_START_DELAY_MAX', 30)))
        logger.info(f"详情任务就绪，随机休眠 {wait_start:.2f} 秒后启动...")
        time.sleep(wait_start)

        try:
            self._set_lock(1)
            duration = random.uniform(int(os.getenv('DETAIL_TASK_DURATION_MIN', 15)),
                                      int(os.getenv('DETAIL_TASK_DURATION_MAX', 25)))
            end_time = datetime.now() + timedelta(minutes=duration)
            logger.info(f"本轮任务总时长设定: {duration:.2f} 分钟，预计将在 {end_time.strftime('%H:%M:%S')} 结束。")

            while datetime.now() < end_time:
                tasks = self._get_pending_tasks(batch_size=10)
                if not tasks:
                    logger.info("库中暂无待处理文章，任务结束。")
                    break

                for task in tasks:
                    if datetime.now() >= end_time: break

                    start_time_item = time.time()
                    logger.info(f"正在处理: {task['title']}")

                    try:
                        res = requests.get(task['link'], headers=self.headers, timeout=15)
                        res.encoding = 'utf-8'
                        soup = BeautifulSoup(res.text, 'html.parser')

                        # 1. 提取元数据 (作者、公众号名)
                        author = soup.find('span', id='js_author_name')
                        author = author.get_text(strip=True) if author else "未知作者"
                        account = soup.find('a', id='js_name')
                        account = account.get_text(strip=True) if account else "未知公众号"

                        content_div = soup.find('div', id='js_content')
                        if not content_div:
                            logger.error(f"无法定位正文内容: {task['title']}")
                            conn = self._get_conn()
                            with conn.cursor() as cur:
                                cur.execute("UPDATE articles SET crawl_status = 2 WHERE id = %s", (task['id'],))
                            conn.commit()
                            conn.close()
                            continue

                        # 2. 处理图片：下载并清理多余属性
                        curr_date = datetime.now().strftime('%Y%m%d')
                        for img in content_div.find_all('img'):
                            origin_src = img.get('data-src') or img.get('src')
                            if origin_src:
                                local_rel_path = self._download_img(origin_src, curr_date, task['fakeid'],
                                                                    task['article_sn'])
                                # 清空所有属性，仅保留 src
                                img.attrs = {'src': local_rel_path}

                        # 3. 构建优化的 MD 结构
                        # 模仿微信原生排版：标题 -> 信息栏 -> 分割线 -> 正文
                        md_header = f"# {task['title']}\n\n"
                        md_header += f"**作者**: {author}  |  **公众号**: {account}\n\n"
                        md_header += f"**发布时间**: {task['create_time']}  |  [原文链接]({task['link']})\n\n"
                        md_header += "---\n\n"

                        md_body = self.h2t.handle(str(content_div))
                        full_md = md_header + md_body

                        # 4. 保存文件
                        safe_title = re.sub(r'[\\/:*?"<>|]', '_', task['title'])
                        md_name = f"{safe_title}_{task['article_sn']}.md"
                        md_full_path = os.path.join(MD_DIR, md_name)

                        with open(md_full_path, 'w', encoding='utf-8') as f:
                            f.write(full_md)

                        # 5. 更新数据库[cite: 1]
                        conn = self._get_conn()
                        with conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO article_details (article_id, content_html, content_text, author, md_path) VALUES (%s, %s, %s, %s, %s)",
                                (task['id'], str(content_div), content_div.get_text(), author, md_full_path)
                            )
                            cur.execute("UPDATE articles SET crawl_status = 1 WHERE id = %s", (task['id'],))
                        conn.commit()
                        conn.close()

                        elapsed = time.time() - start_time_item
                        logger.info(f"成功导出 MD: {md_name}，耗时 {elapsed:.2f} 秒。")

                    except Exception as e:
                        logger.error(f"单条处理异常: {task['title']} - {e}")

                    # 间歇休眠
                    if datetime.now() < end_time:
                        sleep_item = random.uniform(int(os.getenv('ARTICLE_SLEEP_MIN', 3)),
                                                    int(os.getenv('ARTICLE_SLEEP_MAX', 10)))
                        logger.info(f"等待 {sleep_item:.2f} 秒后处理下一条...")
                        time.sleep(sleep_item)

        finally:
            self._set_lock(0)
            logger.info("任务结束，释放运行锁。")


if __name__ == "__main__":
    crawler = WeChatDetailCrawler()
    crawler.run()