import pymysql
import os
from dotenv import load_dotenv

def init_db():
    load_dotenv()
    conn = pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        port=int(os.getenv('DB_PORT', 3306)),
        charset='utf8mb4'
    )
    try:
        with conn.cursor() as cursor:
            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('DB_NAME')} DEFAULT CHARACTER SET utf8mb4;")
            conn.select_db(os.getenv('DB_NAME'))

            # 1. 关键词表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `crawl_keywords` (
                    `id` int(11) NOT NULL AUTO_INCREMENT,
                    `keyword` varchar(100) NOT NULL,
                    `is_active` tinyint(1) DEFAULT '1',
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_keyword` (`keyword`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 2. 授权信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `auth_credentials` (
                    `account` varchar(50) NOT NULL,
                    `token` text NOT NULL,
                    `expire_time` datetime DEFAULT NULL,
                    PRIMARY KEY (`account`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 3. 分类统计表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `category_stats` (
                    `keyword_id` int(11) NOT NULL,
                    `category_type` varchar(50) NOT NULL,
                    `total_count_api` int(11) DEFAULT '0',
                    `crawled_count` int(11) DEFAULT '0',
                    `last_page_index` int(11) DEFAULT '1',
                    `status` tinyint(1) DEFAULT '0' COMMENT '0:全量中, 1:已完成',
                    PRIMARY KEY (`keyword_id`,`category_type`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 4. 数据列表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `data_list` (
                    `id` int(11) NOT NULL AUTO_INCREMENT,
                    `post_id` varchar(255) NOT NULL,
                    `keyword_id` int(11) DEFAULT NULL,
                    `category` varchar(50) DEFAULT NULL,
                    `title` varchar(500) DEFAULT NULL,
                    `pub_time` datetime DEFAULT NULL,
                    `detail_status` tinyint(1) DEFAULT '0' COMMENT '0:待抓, 1:成功, 2:失败',
                    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    KEY `idx_post_id` (`post_id`),
                    KEY `idx_title` (`title`(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 5. 数据详情
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `data_details` (
                    `post_id` varchar(255) NOT NULL,
                    `content_text` longtext,
                    `file_paths` text COMMENT '逗号分隔路径',
                    `has_pdf` tinyint(1) DEFAULT '0',
                    `pdf_status` tinyint(1) DEFAULT '0',
                    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (`post_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            print(">>> 数据库表结构初始化成功！")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()