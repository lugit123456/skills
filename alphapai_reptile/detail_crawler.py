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
    # 详情接口配置常量
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
        """清洗文件名中的非法字符"""
        if not filename: return str(int(time.time()))
        filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
        return filename.strip().replace('\n', '')[:100]

    def parse_ai_summary(self, ai_data):
        """解析 aiSummaryV3 里的 topicBullets (二维数组)"""
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
            if texts:
                all_topics.append(f"{title}：{'；'.join(texts)}")
        return "\n".join(all_topics)

    def parse_mt_summary(self, summary_obj):
        """深度解析会议/图表中的 content 列表数据"""
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
        """获取详情接口数据"""
        cfg = self.DETAIL_API_CONFIG.get(category)
        if not cfg: return None
        params = {cfg["id_param"]: post_id}
        if cfg.get("need_word"): params["word"] = keyword
        return self.safe_request(cfg["method"], cfg["url"], params=params, headers=self.get_headers(token, keyword))

    def fetch_pdf_path_api(self, post_id, token):
        """调用接口获取 PDF 相对路径"""
        url = "https://alphapai-web.rabyte.cn/external/alpha/api/reading/report/detail/pdf"
        params = {"id": post_id, "version": 1}
        res = self.safe_request("GET", url, params=params, headers=self.get_headers(token))
        if res and res.get("code") == 200000:
            return res.get("data")
        return None

    def download_file(self, url, sub_dir="images", custom_name=None, suffix=""):
        """下载文件并按 标题+时间 命名"""
        if not url or not url.startswith("http"): return None
        try:
            root = os.getenv("DATA_ROOT", "data_storage")
            date_folder = datetime.now().strftime("%Y%m%d")
            save_path = os.path.join(root, sub_dir, date_folder)
            if not os.path.exists(save_path): os.makedirs(save_path)

            ext = os.path.splitext(url.split('?')[0])[1] or (".pdf" if sub_dir == "pdfs" else ".png")
            file_name = f"{custom_name}{suffix}{ext}" if custom_name else f"{int(time.time())}{ext}"

            local_full_path = os.path.join(save_path, file_name)
            resp = self.session.get(url, headers=self.get_headers(), timeout=60, stream=True)
            if resp.status_code == 200:
                with open(local_full_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
                return os.path.relpath(local_full_path, root)
        except Exception as e:
            self.logger.error(f"下载失败 [{url}]: {str(e)}")
        return None

    def run_task(self):
        """详情爬取主逻辑"""
        if self.is_quiet_period(): return
        token = self.get_token()
        if not token: return

        duration = random.randint(10, 20)
        end_time = time.time() + duration * 60
        self.logger.info(f"详情任务开始，预计执行 {duration} 分钟")

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
                    if not tasks: break

                    for t in tasks:
                        pid, cat, kwd = t['post_id'], t['category'], t['keyword']
                        self.logger.info(f"[*] 正在抓取详情: {pid} [{cat}]")

                        res = self.fetch_detail_api(pid, cat, kwd, token)
                        if res == "RELOGIN":
                            token = self.get_token()
                            continue

                        if not res or not isinstance(res, dict) or res.get("code") != 200000:
                            err_msg = res.get('message') if isinstance(res, dict) else "接口未响应"
                            self.logger.error(f"  [!] 接口请求失败: {pid} | 原因: {err_msg}")
                            cursor.execute("UPDATE data_list SET detail_status = 2 WHERE id = %s", (t['id']))
                            conn.commit()
                            continue

                        data = res.get("data", {})
                        raw_title = data.get("title") or data.get("figureTitle") or "无标题"
                        pub_date = (data.get("publishTime") or data.get("date") or "")[:10]
                        base_name = self.sanitize_filename(f"{raw_title}_{pub_date}")

                        content_text, file_paths, has_pdf = "", [], 0

                        # 1. 检查 AI 摘要
                        ai_summary = self.parse_ai_summary(data.get("aiSummaryV3"))

                        # 2. PDF 获取逻辑 (直接拼写真实存储地址，不请求 HTML 预览页面)
                        pdf_flag = data.get("pdfFlag")
                        pdf_url = data.get("pdfUrl")

                        if (pdf_flag is True or str(pdf_flag).lower() == "true") and not pdf_url:
                            self.logger.info("  [PDF] 触发 pdfFlag，正在获取相对路径...")
                            rel_path = self.fetch_pdf_path_api(pid, token)
                            if rel_path:
                                # 将路径中的中文进行 URL 编码，保留斜杠 /
                                safe_rel_path = urllib.parse.quote(rel_path.lstrip('/'), safe='/')
                                # 拼接 storage 域名和 JWT authorization
                                pdf_url = f"https://alphapai-storage.rabyte.cn/report/{safe_rel_path}?authorization={token}&platform=web"

                        # 3. HTML 正文获取逻辑
                        raw_content = data.get("content") or data.get("htmlContent") or data.get("html")
                        if not raw_content:
                            if cat == "会议":
                                raw_content = self.parse_mt_summary(data.get("mtSummarySwitchOpen"))
                            elif cat == "图表":
                                raw_content = self.parse_mt_summary(data.get("result"))

                        # 4. 执行文件下载与文本提取
                        if pdf_url:
                            path = self.download_file(pdf_url, "pdfs", custom_name=base_name)
                            if path: file_paths.append(path); has_pdf = 1
                            content_text = raw_content or raw_title
                        elif raw_content:
                            soup = BeautifulSoup(raw_content, "lxml")
                            img_idx = 1
                            for img in soup.find_all("img"):
                                src = img.get("src") or img.get("data-src")
                                path = self.download_file(urljoin("https://alphapai-web.rabyte.cn", src), "images",
                                                          custom_name=base_name, suffix=f"_{img_idx}")
                                if path: file_paths.append(path); img_idx += 1
                            for s in soup(["script", "style"]): s.decompose()
                            content_text = soup.get_text(separator="\n", strip=True)
                        else:
                            content_text = raw_title

                        # 5. 合并并入库
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
                        time.sleep(random.uniform(2, 5))
        finally:
            conn.close()


if __name__ == "__main__":
    crawler = DetailCrawler(os.getenv('REPTILE_DETAILS_LOG_DIR', 'logs/details'))
    crawler.run_task()