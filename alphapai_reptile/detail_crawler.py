import os
import time
import random
import re
import json
import urllib.parse
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from common import BaseCrawler, datetime


class DetailCrawler(BaseCrawler):
    DETAIL_API_CONFIG = {
        "公告": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/announcement/detail",
                 "id_param": "id", "method": "GET"},
        "点评": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/comment/detail",
                 "id_param": "commentId", "method": "GET", "need_word": True},
        "个股": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/stock/detail", "id_param": "id",
                 "method": "GET"},
        "会议": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/roadshow/summary/detail",
                 "id_param": "id", "method": "GET"},
        "研报": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/report/detail", "id_param": "id",
                 "method": "GET"},
        "图表": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/report/detail", "id_param": "id",
                 "method": "GET"},
        "社媒": {"url": "https://alphapai-web.rabyte.cn/external/alpha/api/reading/social/media/wechat/article/detail",
                 "id_param": "id", "method": "GET"}
    }

    def sanitize_filename(self, filename):
        if not filename: return str(int(time.time()))
        filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
        return filename.strip().replace('\n', '')[:100]

    def parse_ai_summary(self, ai_data):
        if isinstance(ai_data, str):
            try:
                ai_data = json.loads(ai_data)
            except:
                return ""
        if not ai_data or not isinstance(ai_data, dict): return ""
        topic_bullets = ai_data.get("topicBullets")
        if not topic_bullets or not isinstance(topic_bullets, list): return ""
        all_topics = []
        for topic in topic_bullets:
            title = topic.get("title", "").strip()
            bullets = topic.get("bullets", [])
            texts = [b.get("text", "").strip() for b in bullets if isinstance(b, dict) and b.get("text")]
            if texts: all_topics.append(f"{title}：{'；'.join(texts)}")
        return "\n".join(all_topics)

    def parse_mt_summary(self, summary_obj):
        if not summary_obj: return ""
        if isinstance(summary_obj, str):
            try:
                summary_obj = json.loads(summary_obj)
            except:
                return ""
        content_list = summary_obj.get("content") if isinstance(summary_obj, dict) else summary_obj
        if not isinstance(content_list, list): return ""
        return "".join([str(item.get("content", "")) for item in content_list if isinstance(item, dict)])

    def fetch_detail_api(self, post_id, category, keyword, token):
        cfg = self.DETAIL_API_CONFIG.get(category)
        if not cfg: return None
        params = {cfg["id_param"]: post_id}
        if cfg.get("need_word"): params["word"] = keyword
        return self.safe_request(cfg["method"], cfg["url"], params=params, headers=self.get_headers(token, keyword))

    def fetch_pdf_path_api(self, post_id, token):
        url = "https://alphapai-web.rabyte.cn/external/alpha/api/reading/report/detail/pdf"
        params = {"id": post_id, "version": 1}
        res = self.safe_request("GET", url, params=params, headers=self.get_headers(token))
        if res and res.get("code") == 200000: return res.get("data")
        return None

    def download_file(self, url, keyword, category, custom_name=None, suffix="", ext_override=None):
        """核心重构：文件保存路径变更为 关键词/类型/时间/标题+时间.后缀"""
        if not url or not url.startswith("http"): return None
        try:
            root = os.getenv("DATA_ROOT", "files")
            date_folder = datetime.now().strftime("%Y-%m-%d")
            # 构建新路径: files/德福科技/研报/2026-05-11
            save_path = os.path.join(root, keyword, category, date_folder)
            if not os.path.exists(save_path): os.makedirs(save_path)

            ext = ext_override if ext_override else os.path.splitext(url.split('?')[0])[1]
            if not ext: ext = ".png"  # 兜底扩展名

            file_name = f"{custom_name}{suffix}{ext}" if custom_name else f"{int(time.time())}{ext}"
            local_full_path = os.path.join(save_path, file_name)

            resp = self.session.get(url, headers=self.get_headers(), timeout=60, stream=True)
            if resp.status_code == 200:
                with open(local_full_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
                return os.path.relpath(local_full_path, root)
        except Exception as e:
            self.logger.error(f"下载文件失败 [{url}]: {str(e)}")
        return None

    def run_task(self):
        if self.is_quiet_period(): return
        self.apply_start_delay('DETAIL_START_DELAY_MIN', 'DETAIL_START_DELAY_MAX')

        token = self.get_token()
        if not token: return

        duration = random.randint(int(os.getenv('DETAIL_TASK_DURATION_MIN', 1)),
                                  int(os.getenv('DETAIL_TASK_DURATION_MAX', 2)))
        end_time = time.time() + duration * 60
        self.logger.info(
            f"=== 详情同步任务启动 | 计划执行 {duration} 分钟 | 预计 {datetime.fromtimestamp(end_time).strftime('%H:%M:%S')} 结束 ===")

        conn = self.get_db_conn()
        try:
            while time.time() < end_time:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT dl.id, dl.post_id, dl.category, kw.keyword 
                        FROM data_list dl 
                        JOIN crawl_keywords kw ON dl.keyword_id = kw.id 
                        WHERE dl.detail_status = 0 LIMIT 5
                    """)
                    tasks = cursor.fetchall()
                    if not tasks:
                        self.logger.info("[*] 暂无待处理的详情任务，退出循环。")
                        break

                    for t in tasks:
                        pid, cat, kwd = t['post_id'], t['category'], t['keyword']
                        self.logger.info(f"[*] 解析开始: ID={pid} | 关键词={kwd} | 分类={cat}")

                        res = self.fetch_detail_api(pid, cat, kwd, token)
                        if res == "RELOGIN":
                            self.logger.warning("  [!] Token 已在服务端失效，正在强制清理本地凭证并重新登录...")
                            # 核心修复：强制删除本地旧 token，打破死循环
                            cursor.execute("DELETE FROM auth_credentials")
                            conn.commit()
                            token = self.get_token()
                            continue

                        if not res or not isinstance(res, dict) or res.get("code") != 200000:
                            err_msg = res.get('message') if isinstance(res, dict) else "接口未响应"
                            self.logger.error(f"  [X] 解析失败: {err_msg}")
                            cursor.execute("UPDATE data_list SET detail_status = 2 WHERE id = %s", (t['id']))
                            conn.commit()
                            continue

                        data = res.get("data", {})
                        raw_title = data.get("title") or data.get("figureTitle") or "无标题"
                        pub_date = (data.get("publishTime") or data.get("date") or "")[:10]
                        # 构造文件名基础: 标题_日期
                        base_name = self.sanitize_filename(f"{raw_title}_{pub_date}")

                        content_text, file_paths, has_pdf = "", [], 0
                        ai_summary = self.parse_ai_summary(data.get("aiSummaryV3"))

                        pdf_flag = data.get("pdfFlag")
                        pdf_url = data.get("pdfUrl")

                        if (pdf_flag is True or str(pdf_flag).lower() == "true") and not pdf_url:
                            rel_path = self.fetch_pdf_path_api(pid, token)
                            if rel_path:
                                safe_rel_path = urllib.parse.quote(rel_path.lstrip('/'), safe='/')
                                pdf_url = f"https://alphapai-storage.rabyte.cn/report/{safe_rel_path}?authorization={token}&platform=web"

                        raw_content = data.get("content") or data.get("htmlContent") or data.get("html")
                        if not raw_content:
                            if cat == "会议":
                                raw_content = self.parse_mt_summary(data.get("mtSummarySwitchOpen"))
                            elif cat == "图表":
                                raw_content = self.parse_mt_summary(data.get("result"))

                        if pdf_url:
                            self.logger.info("  [↓] 正在下载 PDF 附件...")
                            path = self.download_file(pdf_url, kwd, cat, custom_name=base_name, ext_override=".pdf")
                            if path:
                                file_paths.append(path)
                                has_pdf = 1
                                self.logger.info(f"  [√] PDF保存成功: {path}")
                            content_text = raw_content or raw_title
                        elif raw_content:
                            soup = BeautifulSoup(raw_content, "lxml")
                            img_idx = 1
                            for img in soup.find_all("img"):
                                src = img.get("src") or img.get("data-src")
                                if src:
                                    img_path = self.download_file(urljoin("https://alphapai-web.rabyte.cn", src), kwd,
                                                                  cat, custom_name=base_name, suffix=f"_{img_idx}")
                                    if img_path:
                                        file_paths.append(img_path)
                                        img_idx += 1
                            for s in soup(["script", "style"]): s.decompose()
                            content_text = soup.get_text(separator="\n", strip=True)
                            self.logger.info(
                                f"  [√] HTML解析完成: 提取纯文本 {len(content_text)} 字，下载配图 {img_idx - 1} 张。")
                        else:
                            content_text = raw_title
                            self.logger.info("  [!] 无正文与附件，仅保存标题。")

                        final_content = content_text
                        if ai_summary:
                            final_content = f"【AI摘要】：\n{ai_summary}\n\n【正文】：\n{content_text}"

                        cursor.execute("""
                            INSERT INTO data_details (post_id, content_text, file_paths, has_pdf)
                            VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE content_text=%s, file_paths=%s, has_pdf=%s
                        """, (pid, final_content, ",".join(file_paths), has_pdf, final_content, ",".join(file_paths),
                              has_pdf))

                        cursor.execute("UPDATE data_list SET detail_status = 1 WHERE id = %s", (t['id']))
                        conn.commit()

                        # 详情处理后的强制休眠
                        sleep_time = random.uniform(int(os.getenv('ARTICLE_SLEEP_MIN', 2)),
                                                    int(os.getenv('ARTICLE_SLEEP_MAX', 5)))
                        self.logger.info(f"  [Zzz] 防封锁休眠: {sleep_time:.2f} 秒...\n")
                        time.sleep(sleep_time)

        finally:
            conn.close()
            self.logger.info("=== 详情抓取任务运行结束 ===")


if __name__ == "__main__":
    crawler = DetailCrawler(os.getenv('REPTILE_DETAILS_LOG_DIR', 'logs/details'))
    crawler.run_task()