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
                    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增ID',
                    `keyword` varchar(100) NOT NULL COMMENT '需要抓取的关键词',
                    `is_active` tinyint(1) DEFAULT '1' COMMENT '是否激活: 1激活, 0禁用',
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_keyword` (`keyword`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抓取关键词配置表';
            """)

            # 2. 授权信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `auth_credentials` (
                    `account` varchar(50) NOT NULL COMMENT '登录手机号/账号',
                    `token` text NOT NULL COMMENT '认证授权Token',
                    `expire_time` datetime DEFAULT NULL COMMENT 'Token过期时间',
                    PRIMARY KEY (`account`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户认证凭证表';
            """)

            # 3. 分类统计表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `category_stats` (
                    `keyword_id` int(11) NOT NULL COMMENT '关联的关键词ID',
                    `category_type` varchar(50) NOT NULL COMMENT '分类名称(如:研报,公告,会议等)',
                    `total_count_api` int(11) DEFAULT '0' COMMENT 'API接口返回的该分类总条数',
                    `crawled_count` int(11) DEFAULT '0' COMMENT '本地已抓取入库的条数',
                    `last_page_index` int(11) DEFAULT '1' COMMENT '最后一次抓取的页码',
                    `status` tinyint(1) DEFAULT '0' COMMENT '抓取状态: 0全量同步中, 1全量已完成(进入增量)',
                    PRIMARY KEY (`keyword_id`,`category_type`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='各分类抓取进度统计表';
            """)

            # 4. 数据列表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `data_list` (
                    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增ID',
                    `post_id` varchar(255) NOT NULL COMMENT '文章/记录的唯一标识ID(或reportId)',
                    `keyword_id` int(11) DEFAULT NULL COMMENT '归属的关键词ID',
                    `category` varchar(50) DEFAULT NULL COMMENT '文章所属分类',
                    `title` varchar(500) DEFAULT NULL COMMENT '文章清洗后的纯文本标题',
                    `pub_time` datetime DEFAULT NULL COMMENT '文章发布时间',
                    `detail_status` tinyint(1) DEFAULT '0' COMMENT '详情抓取状态: 0待抓, 1成功, 2失败',
                    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '记录入库时间',
                    PRIMARY KEY (`id`),
                    KEY `idx_post_id` (`post_id`),
                    KEY `idx_title` (`title`(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抓取数据列表信息表';
            """)

            # 5. 数据详情
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `data_details` (
                    `post_id` varchar(255) NOT NULL COMMENT '关联列表表的post_id',
                    `content_text` longtext COMMENT '文章正文/AI摘要/图表提取的纯文本内容',
                    `file_paths` text COMMENT '本地附件路径(逗号分隔)',
                    `has_pdf` tinyint(1) DEFAULT '0' COMMENT '是否包含PDF附件: 1是, 0否',
                    `pdf_status` tinyint(1) DEFAULT '0' COMMENT 'PDF下载状态(预留)',
                    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '详情入库时间',
                    PRIMARY KEY (`post_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文章详情及附件关联表';
            """)
            print(">>> 数据库表结构初始化成功！所有字段已添加规范注释。")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()