import sys
import os
import time
import random
from bs4 import BeautifulSoup
from common import BaseCrawler, datetime


class ListCrawler(BaseCrawler):
    CATEGORY_MAP = {
        "comment": "点评", "report": "研报", "announcement": "公告",
        "roadshowSummary": "会议", "figure": "图表", "socialMediaIndividual": "社媒"
    }

    API_CONFIG = {
        "点评": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/comment/search", "method": "POST",
                 "ps": 30},
        "个股": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/stock/search", "method": "GET",
                 "ps": 30},
        "会议": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/roadshow/summary/search",
                 "method": "POST", "ps": 30, "ext": {"sortType": 0, "type": 31, "isPrivateSearch": "0"}},
        "研报": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/report/search", "method": "POST",
                 "ps": 15, "ext": {"marketType": 0}},
        "公告": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/announcement/search",
                 "method": "POST", "ps": 30},
        "图表": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/figure/searchV2", "method": "POST",
                 "ps": 10},
        "社媒": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/social/media/search/article",
                 "method": "POST", "ps": 20}
    }

    def clean_html(self, raw_html):
        if not raw_html: return ""
        return BeautifulSoup(raw_html, "lxml").get_text(strip=True)

    def call_summary_api(self, keyword, token):
        url = "https://alphapai-web.rabyte.cn/external/alpha/api/reading/smartSearch"
        payload = {"word": keyword, "reportSource": True}
        res = self.safe_request("POST", url, json=payload, headers=self.get_headers(token, keyword))
        if res == "RELOGIN": return "RELOGIN"

        results = []
        if res and isinstance(res, dict) and res.get('code') == 200000 and res.get('data'):
            data = res['data']
            for api_key, show_name in self.CATEGORY_MAP.items():
                if show_name == "社媒": continue
                if api_key in data and data[api_key]:
                    results.append({'name': show_name, 'total': data[api_key].get('total', 0)})

        smedia_res = self.fetch_list_data("社媒", keyword, 1, token)
        if smedia_res == "RELOGIN": return "RELOGIN"
        if smedia_res and isinstance(smedia_res, dict) and smedia_res.get('code') == 200000:
            total_count = smedia_res.get('data', {}).get('totalCount', 0)
            results.append({'name': '社媒', 'total': total_count})
        return results

    def fetch_list_data(self, cat_name, word, page, token):
        cfg = self.API_CONFIG.get(cat_name)
        if not cfg: return None
        params = {"word": word, "pageNum": page, "pageSize": cfg['ps']}
        if "ext" in cfg: params.update(cfg['ext'])
        if cfg['method'] == "GET":
            return self.safe_request("GET", cfg['url'], params=params, headers=self.get_headers(token, word))
        else:
            return self.safe_request("POST", cfg['url'], json=params, headers=self.get_headers(token, word))

    def crawl_category_list(self, cursor, kw_id, keyword, cat_name, stat, token, end_time):
        is_inc = (stat['status'] == 1)
        page = 1 if is_inc else stat['last_page_index']
        ps = self.API_CONFIG[cat_name]['ps']
        last_page_first_id = None

        while time.time() < end_time:
            self.logger.info(
                f"  [+] 抓取 [{keyword}] -> [{cat_name}] | 第 {page} 页 {'(增量)' if is_inc else '(全量)'}")
            resp = self.fetch_list_data(cat_name, keyword, page, token)

            # --- 核心修复 1：拦截 RELOGIN，强制删表并更新 Token ---
            if resp == "RELOGIN":
                self.logger.warning(f"  [!] Token 在列表抓取时失效，正在强制清理本地凭证并重新登录...")
                cursor.execute("DELETE FROM auth_credentials")
                cursor.connection.commit()
                token = self.get_token()
                if not token: break  # 如果实在拿不到Token，跳出当前分类
                continue  # 拿到新Token后，重新请求当前页

            # --- 核心修复 2：增加 isinstance 防御，防止非字典对象报错 ---
            if not resp or not isinstance(resp, dict) or resp.get('code') != 200000:
                self.logger.error(f"  [X] {cat_name} 接口请求异常，跳出当前分页逻辑")
                break

            data_obj = resp.get('data', {})
            if cat_name == "图表":
                items = data_obj.get('result', {}).get('list') or []
            elif cat_name == "社媒":
                items = data_obj.get('socialMediaSearchArticles') or []
            else:
                items = data_obj.get('list') or []

            if not items:
                self.logger.info(f"  [-] {cat_name} 当前页(第{page}页)无数据返回，列表已触底。")
                cursor.execute("UPDATE category_stats SET status = 1 WHERE keyword_id=%s AND category_type=%s",
                               (kw_id, cat_name))
                cursor.connection.commit()
                break

            current_first_id = items[0].get('id') or items[0].get('reportId')
            if page > 1 and current_first_id == last_page_first_id:
                self.logger.warning(f"  [!] {cat_name} 检测到分页死循环(接口一直返回相同数据)，强制标记完成。")
                cursor.execute("UPDATE category_stats SET status = 1 WHERE keyword_id=%s AND category_type=%s",
                               (kw_id, cat_name))
                cursor.connection.commit()
                break
            last_page_first_id = current_first_id

            ignore_pagination = len(items) > ps
            if ignore_pagination:
                self.logger.warning(
                    f"  [!] {cat_name} 接口存在伪分页(请求{ps}条, 返回了全量{len(items)}条数据)，处理后将跳出。")

            new_inserted_count = 0
            stop_this_category = False

            for item in items:
                pid = item.get('reportId') if cat_name == "图表" else item.get('id')
                raw_title = item.get('figureTitle') if cat_name == "图表" else item.get('title')
                if not pid: continue

                clean_title = self.clean_html(raw_title)
                cursor.execute("SELECT id FROM data_list WHERE post_id=%s OR title=%s LIMIT 1", (pid, clean_title))
                if cursor.fetchone():
                    if is_inc:
                        self.logger.info(f"      => 拦截: 发现重复数据 [{clean_title[:15]}...]，触发增量阻断。")
                        stop_this_category = True
                        break
                    continue

                pub_time = item.get('publishDate') or item.get('date') or item.get('publishTime')
                cursor.execute("""
                        INSERT INTO data_list (post_id, keyword_id, category, title, pub_time)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (pid, kw_id, cat_name, clean_title, pub_time))
                new_inserted_count += 1

            self.logger.info(f"  [√] 本页处理完毕: 接口包含 {len(items)} 条，实际入库 {new_inserted_count} 条新数据。")

            force_finish = 1 if (ignore_pagination or stop_this_category or len(items) < ps) else 0
            cursor.execute("""
                    UPDATE category_stats SET 
                    crawled_count = crawled_count + %s, last_page_index = %s,
                    status = IF(crawled_count >= total_count_api OR %s=1, 1, status)
                    WHERE keyword_id=%s AND category_type=%s
                """, (new_inserted_count, page, force_finish, kw_id, cat_name))
            cursor.connection.commit()

            if force_finish:
                self.logger.info(f"  [-] 满足退出条件(伪分页/增量阻断/数据不足一页)，结束 [{cat_name}] 抓取。")
                break

            page += 1
            sleep_time = random.uniform(int(os.getenv('LINK_SLEEP_TIME_MIN', 2)),
                                        int(os.getenv('LINK_SLEEP_TIME_MAX', 5)))
            self.logger.info(f"  [Zzz] 翻页防风控休眠: {sleep_time:.2f} 秒...\n")
            time.sleep(sleep_time)

        # --- 核心修复 3：将最新的 token 返回，确保下一个分类使用新 Token ---
        return token

    def run(self):
        if self.is_quiet_period(): return
        self.apply_start_delay('START_DELAY_MIN', 'START_DELAY_MAX')

        token = self.get_token()
        if not token:
            self.logger.error("无法获取有效的登录Token，任务终止")
            return

        duration = random.randint(int(os.getenv('RUN_DURATION_MIN', 10)), int(os.getenv('RUN_DURATION_MAX', 20)))
        end_time = time.time() + duration * 60
        self.logger.info(f"=== 列表定时任务启动 | 计划执行 {duration} 分钟 | 预计 {datetime.fromtimestamp(end_time).strftime('%H:%M:%S')} 结束 ===")

        conn = self.get_db_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, keyword FROM crawl_keywords WHERE is_active = 1")
                keywords = cursor.fetchall()

                for kw_item in keywords:
                    if time.time() > end_time: break
                    kw_id, keyword = kw_item['id'], kw_item['keyword']
                    self.logger.info(f"\n==================== 正在处理关键词: 【{keyword}】 ====================")

                    summary_res = self.call_summary_api(keyword, token)
                    if summary_res == "RELOGIN":
                        self.logger.warning("  [!] Token 在综合查询时失效，正在强制清理本地凭证并重新登录...")
                        # 核心修复 4：综合查询阶段也强删 Token 打破死循环
                        cursor.execute("DELETE FROM auth_credentials")
                        conn.commit()
                        token = self.get_token()
                        if not token: continue
                        summary_res = self.call_summary_api(keyword, token)
                        if summary_res == "RELOGIN": continue

                    if not summary_res:
                        self.logger.warning(f"关键词 [{keyword}] 未返回任何统计数据")
                        continue

                    for res in summary_res:
                        cursor.execute("""
                                INSERT INTO category_stats (keyword_id, category_type, total_count_api)
                                VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE total_count_api=%s
                            """, (kw_id, res['name'], res['total'], res['total']))
                    conn.commit()
                    self.logger.info(f"  √ 【{keyword}】 各分类总条数更新完成，准备进入明细列表抓取。")

                    for res in summary_res:
                        if time.time() > end_time:
                            self.logger.info("任务总执行时间已到，准备优雅退出...")
                            break

                        cat_name = res['name']
                        cursor.execute(
                            "SELECT crawled_count, last_page_index, status FROM category_stats WHERE keyword_id=%s AND category_type=%s",
                            (kw_id, cat_name))
                        stat = cursor.fetchone()

                        if stat['status'] == 0 and stat['crawled_count'] >= res['total']:
                            cursor.execute(
                                "UPDATE category_stats SET status=1 WHERE keyword_id=%s AND category_type=%s",
                                (kw_id, cat_name))
                            conn.commit()
                            self.logger.info(
                                f"  [-] {cat_name} 历史已爬满 ({stat['crawled_count']}/{res['total']})，切换为增量监控状态")
                            stat['status'] = 1

                        # 用 token 变量接收函数返回，确保 Token 刷新能传递
                        token = self.crawl_category_list(cursor, kw_id, keyword, cat_name, stat, token, end_time)

                        # =======================================================
                        # 核心修复：分类切换防风控休眠
                        # 防止增量阻断时 break 语句跳过了翻页休眠，导致分类间请求出现毫秒级连发
                        # =======================================================
                        cat_sleep = random.uniform(int(os.getenv('LINK_SLEEP_TIME_MIN', 2)),
                                                   int(os.getenv('LINK_SLEEP_TIME_MAX', 5)))
                        self.logger.info(
                            f"  [Zzz] [{cat_name}] 分类处理完毕，切换下一分类前休眠: {cat_sleep:.2f} 秒...\n")
                        time.sleep(cat_sleep)

                        # 关键词切换前的休眠
                    break_sleep = random.uniform(int(os.getenv('BREAK_TIME_MIN', 1)),
                                                 int(os.getenv('BREAK_TIME_MAX', 4)))
                    self.logger.info(
                        f"\n  [Zzz] 关键词 【{keyword}】 处理完毕，切换下一关键词前强制休眠: {break_sleep:.2f} 秒...\n")
                    time.sleep(break_sleep)

        finally:
            conn.close()
            self.logger.info("=== 数据库连接已关闭，本次列表任务运行结束 ===")


if __name__ == "__main__":
    log_path = os.getenv('SCHEDULER_SKILL_LOG_DIR', 'logs/scheduler')
    ListCrawler(log_path).run()