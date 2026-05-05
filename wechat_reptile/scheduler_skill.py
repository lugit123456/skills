import os
import time
import random
import logging
import pymysql
from datetime import datetime, timedelta, time as dt_time
from dotenv import load_dotenv
from wechat_fetcher import fetch_batch_articles, get_valid_auth, DB_CONFIG

# 获取 .env 所在目录作为基准路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


# --- 1. 路径处理函数 (支持绝对/相对路径判定) ---
def get_path(env_key, default_rel_path):
    """
    1. 如果 .env 中配置的是绝对路径，直接返回该路径
    2. 如果是相对路径，则与脚本所在目录 BASE_DIR 拼接
    """
    path_val = os.getenv(env_key, default_rel_path)
    if os.path.isabs(path_val):
        full_path = path_val
    else:
        full_path = os.path.normpath(os.path.join(BASE_DIR, path_val))

    if not os.path.exists(full_path):
        os.makedirs(full_path, exist_ok=True)
    return full_path


# --- 2. 日志系统初始化 ---
LOG_DIR = get_path('SCHEDULER_LOG_DIR', 'logs/scheduler_skill')
log_filename = os.path.join(LOG_DIR, f"scheduler_skill_{datetime.now().strftime('%Y%m%d')}.log")

logger = logging.getLogger('wechat_spider')
logger.setLevel(logging.INFO)

# 清理现有的 handler，防止重复打印[cite: 2]
if logger.handlers:
    logger.handlers.clear()

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 文件 Handler：保存到配置的目录[cite: 2]
fh = logging.FileHandler(log_filename, encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

# 控制台 Handler：实时显示进度[cite: 2]
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def _cleanup_old_logs(retention_days=7):
    """自动清理旧日志[cite: 2]"""
    try:
        now = time.time()
        for filename in os.listdir(LOG_DIR):
            if filename.endswith(".log"):
                file_path = os.path.join(LOG_DIR, filename)
                if os.path.getmtime(file_path) < now - (retention_days * 86400):
                    os.remove(file_path)
    except Exception as e:
        logger.error(f"清理日志失败: {e}")


# 执行清理任务
_cleanup_old_logs()


# --- 3. 辅助功能函数 ---[cite: 2]

def set_lock(status):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE wechat_config SET is_running = %s WHERE id = 1", (status,))
    conn.commit()
    conn.close()


def get_target_bloggers():
    """全员完成后自动重启循环[cite: 2]"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    cursor.execute("SELECT fakeid, nickname, last_begin FROM bloggers WHERE is_finished = 0")
    targets = cursor.fetchall()

    if not targets:
        logger.info(">>> 所有博主已处理完，重置进度开启新一轮日常轮询...")
        cursor.execute("UPDATE bloggers SET is_finished = 0, last_begin = 0")
        conn.commit()
        cursor.execute("SELECT fakeid, nickname, last_begin FROM bloggers")
        targets = cursor.fetchall()

    conn.close()
    return targets


def is_quiet_time():
    """静默期判定[cite: 2]"""
    now = datetime.now().time()

    def get_time(key, default):
        t_str = os.getenv(key, default)
        h, m = map(int, t_str.split(':'))
        return dt_time(h, m)

    q_start = get_time("QUIET_START", "00:00")
    q_end = get_time("QUIET_END", "00:00")

    if q_start > q_end:
        return now >= q_start or now < q_end
    return q_start <= now < q_end


# --- 4. 主运行逻辑 ---[cite: 2]

def run_skill():
    if is_quiet_time():
        logger.info(f"🌙 当前处于静默期 ({os.getenv('QUIET_START')} - {os.getenv('QUIET_END')})，跳过任务。")
        return

    # 随机启动延迟
    start_delay = random.uniform(float(os.getenv('START_DELAY_MIN', 0)), float(os.getenv('START_DELAY_MAX', 3)))
    logger.info(f"随机启动延时 {start_delay} 秒...")
    time.sleep(start_delay)

    try:
        set_lock(1)
        bloggers = get_target_bloggers()
        skill_start_time = time.time()
        # 总执行时长限制[cite: 2]
        max_duration = random.uniform(float(os.getenv('RUN_DURATION_MIN', 11)),
                                      float(os.getenv('RUN_DURATION_MAX', 17))) * 60

        for blogger in bloggers:
            if (time.time() - skill_start_time) > max_duration:
                logger.info("已达到 Skill 本次运行时间上限，准备释放锁退出。")
                break

            logger.info(f"--- 目标: {blogger['nickname']} ---")
            token, cookie = get_valid_auth()
            fetcher = fetch_batch_articles(blogger['fakeid'], blogger['nickname'], token, cookie)

            for result in fetcher:
                if result['status'] in ["finished", "exists"]:
                    break
                if (time.time() - skill_start_time) > max_duration:
                    break

            # 切换博主前休息[cite: 2]
            rest = random.uniform(float(os.getenv('BREAK_TIME_MIN', 1)), float(os.getenv('BREAK_TIME_MAX', 4)))
            logger.info(f"休眠 {rest:.2f} 秒后切换博主...")
            time.sleep(rest)

    finally:
        set_lock(0)
        logger.info("任务结束，锁已释放。")


if __name__ == "__main__":
    run_skill()