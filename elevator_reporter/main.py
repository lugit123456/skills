import os
import json
import sys
import pandas as pd
import mysql.connector
import oss2
from datetime import datetime
from dotenv import load_dotenv

# 加载配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


def upload_to_oss(local_file_path, region):
    """上传文件到阿里云 OSS 并返回 HTTPS URL"""
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
    # 确保 endpoint 不带协议头，后面手动拼接 https
    endpoint = os.getenv("OSS_ENDPOINT").replace("https://", "").replace("http://", "")
    bucket_name = os.getenv("OSS_BUCKET_NAME")

    # 初始化 Bucket
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, f"https://{endpoint}", bucket_name)

    # 1. 获取当前时间用于路径
    now = datetime.now()
    date_path = now.strftime("%Y-%m/%d")

    # 2. 按照要求格式命名文件名: Elevator_Report_普陀_20260313_143824.xlsx
    time_str = now.strftime("%Y%m%d_%H%M%S")
    file_name = f"Elevator_Report_{region}_{time_str}.xlsx"

    # 3. 构建 OSS 完整存放路径
    oss_file_path = f"{date_path}/{file_name}"

    # 4. 执行上传
    with open(local_file_path, 'rb') as fileobj:
        bucket.put_object(oss_file_path, fileobj)

    # 5. 返回 HTTPS 链接
    url = f"https://{bucket_name}.{endpoint}/{oss_file_path}"
    return url


def run_skill():
    conn = None
    local_file_path = None
    try:
        if len(sys.argv) < 2:
            return

        input_data = json.loads(sys.argv[1])
        region = input_data.get("region", "未知地区")
        start_date = input_data.get("start_date")
        end_date = input_data.get("end_date")

        # 数据库配置
        db_config = {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "auth_plugin": "mysql_native_password"
        }

        conn = mysql.connector.connect(**db_config)

        # SQL 查询
        sql = """
        SELECT
            tr.id AS 'ID',
            tr.eleNo AS '设备编号',
            tr.status AS '状态',
            tr.testResult AS '检测结果',
            tr.testDate AS '检测日期',
            be.checkDateNext AS '下次检测日期'
        FROM
            test_report AS tr
            JOIN base_elevator AS be ON tr.eleId = be.id 
        WHERE
            tr.orderType = 1
            AND tr.isDel = 0
            AND be.equlocarecodName LIKE %s
            AND be.checkDateNext >= %s
            AND be.checkDateNext <= %s
        GROUP BY tr.eleNo
        """

        df = pd.read_sql(sql, conn, params=(f"{region}%", start_date, end_date))

        if df.empty:
            print(f"⚠️ 在 {start_date} 到 {end_date} 期间，{region} 地区未查询到数据。")
            return

        # 1. 先保存到本地临时文件
        local_file_path = os.path.join(BASE_DIR, f"temp_export_{region}.xlsx")
        df.to_excel(local_file_path, index=False, engine='openpyxl')

        # 2. 上传到 OSS 并获取 HTTPS 地址
        oss_url = upload_to_oss(local_file_path, region)

        # 3. 输出给 OpenClaw (飞书机器人会抓取这段文字)
        print(f"✅ 报表导出成功！\n地区：{region}\n周期：{start_date} 至 {end_date}\n下载地址：{oss_url}")

    except Exception as e:
        print(f"❌ 运行失败: {str(e)}")
    finally:
        # 关闭数据库
        if conn and conn.is_connected():
            conn.close()
        # 彻底清理本地文件
        if local_file_path and os.path.exists(local_file_path):
            os.remove(local_file_path)


if __name__ == "__main__":
    run_skill()