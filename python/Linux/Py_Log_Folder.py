import os
import shutil
from datetime import datetime

def move_log_files():
    ##工作目錄
    base_dir = "/opt/airflow/dags"
    ##取得今日日期 yyyyMMdd
    today = datetime.now().strftime("%Y%m%d")
    ##建立目標資料夾路徑
    log_dir = os.path.join(base_dir, f"LOG_{today}")
    ##如果今日目標資料夾不存在，才建立
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    ##檢查base_dir底下的所有檔案
    for file_name in os.listdir(base_dir):
        if file_name.endswith("_LOG.txt"):
            src = os.path.join(base_dir, file_name)
            dst = os.path.join(log_dir, file_name)
            shutil.move(src, dst)

if __name__ == "__main__":
    move_log_files()