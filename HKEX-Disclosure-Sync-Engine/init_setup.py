import os
import mysql.connector
from dotenv import load_dotenv

# 路径安全判定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


def setup():
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASS', '')
    }
    db_name = os.getenv('DB_NAME', 'disclosure_sync_manager')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 1. 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
        cursor.execute(f"USE `{db_name}`;")
        print(f"[*] 数据库 `{db_name}` 已就绪")

        # 2. 创建数据表 (V13 版本)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `hkex_disclosures` (
          `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
          `form_serial_no` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '表格序號',
          `fingerprint` varchar(32) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '数据指纹',
          `detail_url` varchar(500) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '主详情链接',
          `assoc_corp_url` varchar(500) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '相關法團详情链接',
          `event_date` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '原始日期',
          `event_date_formatted` DATE DEFAULT NULL COMMENT '标准日期',
          `corp_name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '上市法團名稱',
          `person_name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '大股東/董事名称',
          `reason_text` text COLLATE utf8mb4_bin COMMENT '披露原因',
          `shares_involved` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '買入／賣出股數',
          `avg_price` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '每股平均價',
          `total_shares` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '持有權益总数',
          `share_percentage` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '佔百分比',
          `assoc_corp_interests` text COLLATE utf8mb4_bin COMMENT '相關法團文本',
          `debenture_interests` text COLLATE utf8mb4_bin COMMENT '債權證權益',
          `crawl_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_serial` (`form_serial_no`),
          UNIQUE KEY `idx_fingerprint` (`fingerprint`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
        """)
        print("[*] 数据表 `hkex_disclosures` 已就绪")

        # 3. 创建状态表
        cursor.execute("""
        CREATE TABLE `crawler_status` (
          `id` int(11) NOT NULL COMMENT '状态记录ID',
          `is_running` tinyint(1) DEFAULT '0' COMMENT '原子任务锁 (1:运行中, 0:空闲)',
          `global_start_date` date DEFAULT NULL COMMENT '全量爬取任务的起始日期',
          `global_target_end` date DEFAULT NULL COMMENT '全量爬取任务的终点日期',
          `current_window_start` date DEFAULT NULL COMMENT '当前日期窗口起始',
          `current_window_end` date DEFAULT NULL COMMENT '当前日期窗口终点',
          `current_page` int(11) DEFAULT '1' COMMENT '当前窗口内已同步的页码',
          `last_run_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后运行时间',
          `last_mode` varchar(20) COLLATE utf8mb4_bin DEFAULT 'BULK' COMMENT '上次运行模式: BULK/INCREMENTAL',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='爬虫任务进度管理表';
        """)

        # 4. 初始化种子数据
        cursor.execute("""
        INSERT IGNORE INTO `crawler_status` 
        (id, global_start_date, global_target_end, current_window_start, current_window_end, current_page, last_mode)
        VALUES (1, '2003-01-01', '2026-12-31', '2003-01-01', '2003-06-30', 1, 'BULK');
        """)

        # 4. 初始化状态记录
        cursor.execute("INSERT IGNORE INTO `crawler_status` (id) VALUES (1);")
        conn.commit()
        print(f"[*] 成功初始化数据库 {db_name}")
        print("[提示] 默认从 2003-01-01 开始 BULK 模式。如需修改，请直接操作 crawler_status 表。")

    except Exception as e:
        print(f"[!] 初始化失败: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

    if __name__ == "__main__":
        setup()