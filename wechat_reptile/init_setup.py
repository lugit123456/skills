import os
import pymysql
from dotenv import load_dotenv

# 获取当前脚本所在目录作为基准路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


def get_db_config():
    """从环境变量获取数据库配置"""
    return {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'db': os.getenv('DB_NAME'),
        'charset': 'utf8mb4'
    }


def create_folders():
    """根据配置文件创建必要的目录"""
    print("--- 正在初始化目录结构 ---")
    # 读取配置，若不存在则使用默认值
    log_dir = os.path.join(BASE_DIR, os.getenv('SCHEDULER_SKILL_LOG_DIR', 'logs/scheduler_skill'))
    data_root = os.path.join(BASE_DIR, os.getenv('DATA_ROOT', 'wechat_detail'))

    # 定义需要创建的子目录[cite: 3]
    folders = [
        log_dir,
        os.path.join(data_root, 'markdowns'),
        os.path.join(data_root, 'images')
    ]

    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"已创建目录: {folder}")
        else:
            print(f"目录已存在: {folder}")


def init_database():
    """执行 SQL 初始化表结构"""
    print("\n--- 正在初始化数据库表结构 ---")
    config = get_db_config()

    # 定义表结构 SQL[cite: 1]
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS `wechat_config` (
          `id` int(11) NOT NULL DEFAULT '1' COMMENT '配置ID',
          `token` varchar(100) DEFAULT NULL COMMENT '登录Token',
          `cookie_str` text COMMENT '登录Cookie',
          `is_running` tinyint(4) DEFAULT '0' COMMENT '列表任务锁',
          `is_detail_running` tinyint(4) DEFAULT '0' COMMENT '详情任务锁',
          `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='爬虫凭证状态表';
        """,
        """
        CREATE TABLE IF NOT EXISTS `bloggers` (
          `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
          `nickname` varchar(100) DEFAULT NULL COMMENT '公众号名称',
          `fakeid` varchar(50) DEFAULT NULL COMMENT '公众号唯一标识',
          `last_begin` int(11) DEFAULT '0' COMMENT '上次爬取的偏移量',
          `is_finished` tinyint(4) DEFAULT '0' COMMENT '本轮是否爬取完毕',
          PRIMARY KEY (`id`),
          UNIQUE KEY `fakeid` (`fakeid`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公众号博主进度表';
        """,
        """
        CREATE TABLE IF NOT EXISTS `articles` (
          `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
          `fakeid` varchar(50) DEFAULT NULL COMMENT '博主唯一标识',
          `article_sn` varchar(50) DEFAULT NULL COMMENT '文章唯一标识',
          `title` varchar(255) DEFAULT NULL COMMENT '文章标题',
          `link` text COMMENT '文章链接',
          `create_time` datetime DEFAULT NULL COMMENT '发布时间',
          `crawl_status` tinyint(4) DEFAULT '0' COMMENT '详情抓取状态(0:待抓取, 1:已抓取, 2:抓取失败)',
          PRIMARY KEY (`id`),
          UNIQUE KEY `article_sn` (`article_sn`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文章连接表';
        """,
        """
        CREATE TABLE IF NOT EXISTS `article_details` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `article_id` int(11) DEFAULT NULL COMMENT '文章连接表id',
          `content_html` longtext COMMENT 'html信息',
          `content_text` longtext COMMENT '文章文案信息',
          `author` varchar(100) DEFAULT NULL COMMENT '作者',
          `md_path` varchar(255) DEFAULT NULL COMMENT 'md文件地址',
          `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          KEY `article_id` (`article_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        # 插入初始化配置记录
        "INSERT IGNORE INTO `wechat_config` (`id`) VALUES (1);"
    ]

    try:
        conn = pymysql.connect(**config)
        with conn.cursor() as cursor:
            for sql in sql_statements:
                cursor.execute(sql)
        conn.commit()
        conn.close()
        print("数据库初始化成功！")
    except Exception as e:
        print(f"数据库初始化失败: {e}")


if __name__ == "__main__":
    # 检查 .env 是否存在
    if not os.path.exists(os.path.join(BASE_DIR, '.env')):
        print("错误: 未找到 .env 配置文件，请先创建并填写数据库及路径信息。")
    else:
        create_folders()
        init_database()
        print("\n所有初始化工作已完成。现在可以告诉 AI 开始同步博主并进行爬取了。")