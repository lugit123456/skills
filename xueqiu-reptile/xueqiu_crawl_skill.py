import os
import re
import json
import time
import random
import logging
import pymysql
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# 加载配置文件
load_dotenv()

# --- 1. 日志系统增强配置 (含自动清理逻辑) ---
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


def _cleanup_old_logs(retention_days=7):
    try:
        now = time.time()
        for filename in os.listdir(LOG_DIR):
            if filename.endswith(".log"):
                file_path = os.path.join(LOG_DIR, filename)
                if os.path.getmtime(file_path) < now - (retention_days * 86400):
                    os.remove(file_path)
    except: pass

# 启动时执行清理
_cleanup_old_logs(retention_days=7)

# 配置日志 Handler
log_filename = os.path.join(LOG_DIR, f"xueqiu_{datetime.now().strftime('%Y%m%d')}.log")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 关键：检查是否已经有了 Handler（防止 Trigger 调用时重复打日志）
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(log_filename, encoding='utf-8')
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

class XueqiuSmartSkill:
    def __init__(self):
        # 初始化配置
        self.db_config = {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("DB_PORT", 43306)),
            "user": os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", "123456"),
            "database": os.getenv("DB_NAME", "local"),
            "charset": "utf8mb4"
        }
        self.user_data_dir = os.path.join(os.path.dirname(__file__),
                                          os.getenv("XUEQIU_USER_DATA_DIR", "xueqiu_user_data"))
        self.feishu_webhook = os.getenv("FEISHU_WEBHOOK", "")
        self.processed_uids = set()

    # --- 2. 数据库基础方法 ---

    def _get_db_conn(self):
        return pymysql.connect(**self.db_config)

    def _get_next_task(self):
        """获取下一个待处理任务：优先处理未完成全量的，且排除本轮已处理的"""
        conn = self._get_db_conn()
        exclude_sql = ""
        if self.processed_uids:
            uids_str = ",".join([str(i) for i in self.processed_uids])
            exclude_sql = f"AND user_id NOT IN ({uids_str})"

        sql = f"""
            SELECT * FROM blogger_tasks 
            WHERE 1=1 {exclude_sql}
            ORDER BY status ASC, priority DESC, last_crawl_time ASC 
            LIMIT 1
        """
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchone()
        finally:
            conn.close()

    def _check_remaining_tasks(self):
        conn = self._get_db_conn()
        uids_str = ",".join([str(i) for i in self.processed_uids]) if self.processed_uids else "0"
        sql = f"SELECT COUNT(*) FROM blogger_tasks WHERE user_id NOT IN ({uids_str})"
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0] > 0
        finally:
            conn.close()

    # --- 3. 状态更新与统计方法 ---

    def _update_checkpoint(self, uid, page):
        conn = self._get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE blogger_tasks SET checkpoint_page=%s WHERE user_id=%s", (page, uid))
            conn.commit()
        finally:
            conn.close()

    def _mark_task_status(self, uid, status, page):
        conn = self._get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blogger_tasks SET status=%s, checkpoint_page=%s, last_crawl_time=NOW() WHERE user_id=%s",
                    (status, page, uid))
            conn.commit()
        finally:
            conn.close()

    def _update_last_time(self, uid):
        conn = self._get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE blogger_tasks SET last_crawl_time=NOW() WHERE user_id=%s", (uid,))
            conn.commit()
        finally:
            conn.close()

    def _sync_total_count(self, uid):
        conn = self._get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM blogger_posts WHERE user_id = %s", (uid,))
                actual_count = cur.fetchone()[0]
                cur.execute("UPDATE blogger_tasks SET total_posts_count = %s WHERE user_id = %s", (actual_count, uid))
                logging.info(f"📊 数据对齐：博主 {uid} 库内当前总计 {actual_count} 条动态")
            conn.commit()
        finally:
            conn.close()

    # --- 4. 采集与告警核心方法 ---

    def _send_feishu_alert(self, title, msg, screenshot_path=None):
        if not self.feishu_webhook: return
        content = [[{"tag": "text", "text": msg}]]
        if screenshot_path:
            content.append([{"tag": "text", "text": f"\n📸 错误截图存放在: {screenshot_path}"}])
        payload = {"msg_type": "post", "content": {"post": {"zh_cn": {"title": f"🚨 {title}", "content": content}}}}
        try:
            requests.post(self.feishu_webhook, json=payload, timeout=10)
        except Exception as e:
            logging.error(f"❌ 飞书告警发送失败: {e}")

    def _fetch_inside_page(self, page, uid, p_num):
        script = """
        async (args) => {
            const [u, p] = args;
            const r = await fetch(`/v4/statuses/user_timeline.json?page=${p}&user_id=${u}`);
            return r.status === 200 ? await r.json() : null;
        }
        """
        try:
            return page.evaluate(script, [uid, p_num])
        except:
            return None

    def _save_data(self, statuses):
        conn = self._get_db_conn()
        new_count = 0
        try:
            with conn.cursor() as cur:
                for s in statuses:
                    sql = """INSERT IGNORE INTO blogger_posts (id, user_id, screen_name, content, stock_codes, stock_names, comment_time) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                    clean_content = re.sub(r'<[^>]+>', '', s.get('description', '')).strip()
                    codes = ",".join(s.get('stockCorrelation', []))
                    names = ",".join(re.findall(r'\$([^$()]+)\((?:SH|SZ|HK)?\d{5,6}\)\$', s.get('text', '')))
                    cur.execute(sql, (s['id'], s['user']['id'], s['user']['screen_name'], clean_content, codes, names,
                                      datetime.fromtimestamp(s['created_at'] / 1000).strftime('%Y-%m-%d %H:%M:%S')))
                    new_count += cur.rowcount
            conn.commit()
            return new_count
        finally:
            conn.close()

    # --- 5. 主执行入口 ---

    def execute(self, run_minutes=None, debug_mode=False):
        # 1. 配置文件自检
        env_path = os.path.join(os.path.dirname(__file__), ".env")

        if not os.path.exists(env_path):
            # 报错时顺便打印出它尝试查找的路径，方便你一眼看穿 CWD 问题
            return f"⚠️ 缺少配置文件：请确保在 {env_path} 路径下存在 .env 文件。"

        load_dotenv(env_path)
        # 2. 模式与限时判定
        is_headless = False if debug_mode else os.getenv("BROWSER_HEADLESS", "True").lower() == "true"
        run_minutes = run_minutes or int(os.getenv("CRAWL_RUN_MINUTES", 40))
        deadline = datetime.now() + timedelta(minutes=run_minutes)

        logging.info(f"🚀 核心 Skill 启动 | 限时: {run_minutes}min | 无头模式: {is_headless}")

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=is_headless,
                args=["--disable-blink-features=AutomationControlled"]
            )
            page = context.new_page()

            # 3. 首次登录新手引导逻辑
            if is_headless and not os.path.exists(os.path.join(self.user_data_dir, "Default")):
                context.close()
                return "🚨 【环境未初始化】检测到你从未登录过。请对我说：『开启调试模式运行雪球采集』，在窗口中完成登录。"

            # 4. 主循环逻辑
            while True:
                if datetime.now() >= deadline:
                    logging.info("⏰ 达到任务限时，安全关闭。")
                    break

                task = self._get_next_task()
                if not task:
                    if len(self.processed_uids) == 0:
                        context.close()
                        return "📭 【任务表为空】请在 `blogger_tasks` 表中添加博主 UID 后再试。"
                    logging.info("🏁 本轮已无可处理的博主，任务圆满结束。")
                    break

                uid, name = task['user_id'], task['screen_name']
                is_incremental = (task['status'] >= 2)
                curr_page = (task['checkpoint_page'] + 1) if not is_incremental else 1

                logging.info(
                    f"🎯 切换博主 -> {name} | {'[增量刷新]' if is_incremental else '[全量同步]'} | 起始页: {curr_page}")

                try:
                    page.goto(f"https://xueqiu.com/u/{uid}", wait_until="domcontentloaded", timeout=25000)
                except:
                    pass

                consecutive_fail_count = 0
                while True:
                    data = self._fetch_inside_page(page, uid, curr_page)

                    if data is None:
                        consecutive_fail_count += 1
                        if consecutive_fail_count >= 3:
                            shot_name = os.path.join(LOG_DIR, f"err_{uid}_{int(time.time())}.png")
                            page.screenshot(path=shot_name)
                            self._send_feishu_alert("采集拦截", f"博主 {name} 连续3次请求失败，可能需要手动滑块验证。",
                                                    shot_name)
                            break
                        time.sleep(20);
                        continue

                    consecutive_fail_count = 0
                    statuses = data.get("statuses", [])

                    if len(statuses) == 0:
                        if not is_incremental: self._mark_task_status(uid, 2, 0)
                        break

                    new_count = self._save_data(statuses)

                    # 增量模式哨兵：发现没有新动态时直接切走
                    if is_incremental and new_count == 0:
                        logging.info(f"🛡️ 哨兵拦截：{name} 已追上最新动态，无需翻页。")
                        break

                    if not is_incremental: self._update_checkpoint(uid, curr_page)

                    if datetime.now() >= deadline:
                        logging.warning(f"⏰ 时限临近！保存博主 {name} 进度中...")
                        break

                    wait_time = random.uniform(8, 14)
                    logging.info(f"📑 {name} 第 {curr_page} 页：获取 {len(statuses)} 条，入库 {new_count} 条。")
                    time.sleep(wait_time)
                    curr_page += 1

                # 博主单次任务结算
                self.processed_uids.add(uid)
                self._update_last_time(uid)
                self._sync_total_count(uid)

                # 任务间歇逻辑
                if datetime.now() < deadline:
                    if self._check_remaining_tasks():
                        sleep_time = random.uniform(30, 60)
                        logging.info(f"⏸️ 任务切换间歇，随机休眠 {sleep_time:.1f}s...")
                        time.sleep(sleep_time)
                    else:
                        break
                else:
                    break

            context.close()
        return len(self.processed_uids)


if __name__ == "__main__":
    XueqiuSmartSkill().execute()