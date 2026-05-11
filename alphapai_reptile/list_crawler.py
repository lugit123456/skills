import sys
import os
import time
import random
from bs4 import BeautifulSoup
from common import BaseCrawler, datetime


class ListCrawler(BaseCrawler):
    # 映射综合查询接口的 Key 到 数据库分类名
    CATEGORY_MAP = {
        "comment": "点评",
        "report": "研报",
        "announcement": "公告",
        "roadshowSummary": "会议",
        "figure": "图表",
        "socialMediaIndividual": "社媒"
    }

    # 各分类接口的详细配置
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
        """去除标题中的HTML标签"""
        if not raw_html:
            return ""
        return BeautifulSoup(raw_html, "lxml").get_text(strip=True)

    def call_summary_api(self, keyword, token):
        """综合查询：获取所有分类的 total，并单独获取社媒的 total"""
        url = "https://alphapai-web.rabyte.cn/external/alpha/api/reading/smartSearch"
        payload = {"word": keyword, "reportSource": True}
        res = self.safe_request("POST", url, json=payload, headers=self.get_headers(token, keyword))

        if res == "RELOGIN":
            return "RELOGIN"

        results = []
        if res and isinstance(res, dict) and res.get('code') == 200000 and res.get('data'):
            data = res['data']
            for api_key, show_name in self.CATEGORY_MAP.items():
                if show_name == "社媒":
                    continue  # 跳过综合查询里的社媒，因为没数据
                if api_key in data and data[api_key]:
                    results.append({'name': show_name, 'total': data[api_key].get('total', 0)})

        # --- 核心修复：单独调用社媒列表接口获取总条数 (totalCount) ---
        smedia_res = self.fetch_list_data("社媒", keyword, 1, token)
        if smedia_res == "RELOGIN":
            return "RELOGIN"
        if smedia_res and isinstance(smedia_res, dict) and smedia_res.get('code') == 200000:
            s_data = smedia_res.get('data', {})
            total_count = s_data.get('totalCount', 0)
            results.append({'name': '社媒', 'total': total_count})

        return results

    def fetch_list_data(self, cat_name, word, page, token):
        """通用列表请求函数"""
        cfg = self.API_CONFIG.get(cat_name)
        if not cfg: return None

        params = {"word": word, "pageNum": page, "pageSize": cfg['ps']}
        if "ext" in cfg: params.update(cfg['ext'])

        if cfg['method'] == "GET":
            return self.safe_request("GET", cfg['url'], params=params, headers=self.get_headers(token, word))
        else:
            return self.safe_request("POST", cfg['url'], json=params, headers=self.get_headers(token, word))

    def crawl_category_list(self, cursor, kw_id, keyword, cat_name, stat, token, end_time):
        """执行翻页抓取与入库（增加三重防死循环检测）"""
        is_inc = (stat['status'] == 1)
        page = 1 if is_inc else stat['last_page_index']
        ps = self.API_CONFIG[cat_name]['ps']

        last_page_first_id = None  # 用于检测接口是否一直吐一样的数据

        while time.time() < end_time:
            self.logger.info(f"  [+] 抓取 {cat_name} | {keyword} | 第 {page} 页 {'(增量模式)' if is_inc else ''}")
            resp = self.fetch_list_data(cat_name, keyword, page, token)

            if not resp or resp.get('code') != 200000:
                self.logger.error(f"  [!] {cat_name} 接口返回异常")
                break

            # --- 核心修复：适配图表、社媒与常规列表的数据层级差异 ---
            data_obj = resp.get('data', {})
            if cat_name == "图表":
                items = data_obj.get('result', {}).get('list') or []
            elif cat_name == "社媒":
                items = data_obj.get('socialMediaSearchArticles') or []
            else:
                items = data_obj.get('list') or []

            # --- 防线 1：接口返回真的为空，数据已完全掏空 ---
            if not items:
                self.logger.info(f"  [-] {cat_name} 第 {page} 页无数据，当前分类抓取彻底结束")
                cursor.execute("UPDATE category_stats SET status = 1 WHERE keyword_id=%s AND category_type=%s",
                               (kw_id, cat_name))
                cursor.connection.commit()
                break

            # --- 防线 2：接口分页失效，陷入死循环一直返回同一页 ---
            current_first_id = items[0].get('id') or items[0].get('reportId')
            if page > 1 and current_first_id == last_page_first_id:
                self.logger.warning(f"  [!] {cat_name} 接口分页失效(无限返回相同数据)，强制标记为已完成")
                cursor.execute("UPDATE category_stats SET status = 1 WHERE keyword_id=%s AND category_type=%s",
                               (kw_id, cat_name))
                cursor.connection.commit()
                break
            last_page_first_id = current_first_id

            # --- 防线 3：接口无视了 pageSize (例如请求10条返回62条) ---
            ignore_pagination = len(items) > ps
            if ignore_pagination:
                self.logger.info(
                    f"  [*] {cat_name} 接口无视 pageSize(请求{ps}条,返回{len(items)}条)，处理完本页将强制结束")

            new_inserted_count = 0
            stop_this_category = False

            for item in items:
                if cat_name == "图表":
                    pid = item.get('reportId')
                    raw_title = item.get('figureTitle')
                else:
                    pid = item.get('id')
                    raw_title = item.get('title')

                if not pid: continue

                clean_title = self.clean_html(raw_title)

                # 双重去重判断
                cursor.execute(
                    "SELECT id FROM data_list WHERE post_id=%s OR title=%s LIMIT 1",
                    (pid, clean_title)
                )

                if cursor.fetchone():
                    if is_inc:
                        self.logger.info(f"  [*] {cat_name} 发现重复数据: {clean_title[:15]}... 增量同步完成")
                        stop_this_category = True
                        break
                    continue

                # 社媒中的时间字段为 publishDate，之前的兼容逻辑已经能够完美获取
                pub_time = item.get('publishDate') or item.get('date') or item.get('publishTime')

                cursor.execute("""
                        INSERT INTO data_list (post_id, keyword_id, category, title, pub_time)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (pid, kw_id, cat_name, clean_title, pub_time))
                new_inserted_count += 1

            force_finish = 1 if (ignore_pagination or stop_this_category or len(items) < ps) else 0

            cursor.execute("""
                    UPDATE category_stats SET 
                    crawled_count = crawled_count + %s, 
                    last_page_index = %s,
                    status = IF(crawled_count >= total_count_api OR %s=1, 1, status)
                    WHERE keyword_id=%s AND category_type=%s
                """, (new_inserted_count, page, force_finish, kw_id, cat_name))
            cursor.connection.commit()

            if force_finish:
                self.logger.info(f"  [-] {cat_name} 列表爬取达到结束条件，准备切换分类")
                break

            page += 1
            time.sleep(
                random.uniform(int(os.getenv('LINK_SLEEP_TIME_MIN', 2)), int(os.getenv('LINK_SLEEP_TIME_MAX', 5))))

    def run(self):
        if self.is_quiet_period(): return

        token = self.get_token()
        if not token:
            self.logger.error("无法获取有效的登录Token，任务终止")
            return

        duration = random.randint(int(os.getenv('RUN_DURATION_MIN', 20)), int(os.getenv('RUN_DURATION_MAX', 30)))
        end_time = time.time() + duration * 60
        self.logger.info(f"任务启动，计划执行 {duration} 分钟，预计于 {datetime.fromtimestamp(end_time)} 结束")

        conn = self.get_db_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, keyword FROM crawl_keywords WHERE is_active = 1")
                keywords = cursor.fetchall()

                for kw_item in keywords:
                    if time.time() > end_time: break
                    kw_id, keyword = kw_item['id'], kw_item['keyword']
                    self.logger.info(f"==================== 正在处理关键词: {keyword} ====================")

                    # 第一步：刷新总条数 (现在包含了单独获取社媒的逻辑)
                    summary_res = self.call_summary_api(keyword, token)

                    if summary_res == "RELOGIN":
                        self.logger.warning("Token 在综合查询时失效，尝试刷新...")
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
                    self.logger.info(f"  √ 各分类总条数更新完成，开始列表抓取...")

                    for res in summary_res:
                        if time.time() > end_time:
                            self.logger.info("任务总执行时间已到，准备优雅退出...")
                            break

                        cat_name = res['name']
                        cursor.execute("""
                                SELECT crawled_count, last_page_index, status 
                                FROM category_stats WHERE keyword_id=%s AND category_type=%s
                            """, (kw_id, cat_name))
                        stat = cursor.fetchone()

                        if stat['status'] == 0 and stat['crawled_count'] >= res['total']:
                            cursor.execute(
                                "UPDATE category_stats SET status=1 WHERE keyword_id=%s AND category_type=%s",
                                (kw_id, cat_name))
                            conn.commit()
                            self.logger.info(f"  [-] {cat_name} 全量已完成，切换为增量监控状态")
                            stat['status'] = 1

                        self.crawl_category_list(cursor, kw_id, keyword, cat_name, stat, token, end_time)

                    time.sleep(random.uniform(int(os.getenv('BREAK_TIME_MIN', 2)), int(os.getenv('BREAK_TIME_MAX', 5))))

        finally:
            conn.close()
            self.logger.info("数据库连接已关闭，脚本运行结束")


if __name__ == "__main__":
    log_path = os.getenv('SCHEDULER_SKILL_LOG_DIR', 'logs/scheduler')
    ListCrawler(log_path).run()