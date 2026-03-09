##======>>>1. IMPORT
from datetime import datetime
from pathlib import Path
import csv
from typing import Tuple, Optional, Dict, Any
import traceback as tb
##======>>>Selenium
from selenium import webdriver
##from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import time
import pymysql
import os
import shutil
##======>>>pandas
import pandas as pd
##======>>>1. IMPORT
##======>>>2. LOG FUNCTION
LOG_FILE_NAME = f"Py_Consumer_Loan_&_Construction_Loan_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.txt"
LOG_FILE_PATH = Path(LOG_FILE_NAME)
def log_record(msg: str):
    """Write Each State's Program Logs, Return Nothing"""
    try:
        with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
            f.write(msg)
    except Exception as e:
        print(f"log_record 寫入失敗: {e}")
##======>>>2. LOG FUNCTION
##======>>>3. LOAD IN PARAMS
def load_params(file_path) -> Tuple[str, Optional[Dict[str, str]]]:
    """Load In Params From CSV file To Connect to MySQL & Search Bar's Params"""
    cond_load_pymysql_params = ""
    try:
        param_dict = dict()
        with open(file_path, mode="r", encoding="utf-8") as param_r:
            reader = csv.DictReader(param_r)
            for row in reader:
                key = row['key']
                value = row['value']
                param_dict[key] = value
            exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
            cond_load_pymysql_params = f"{file_path} Success Loaded at: {exec_time}###\n"
        return cond_load_pymysql_params, param_dict
    except Exception as load_pymysql_params_ex:
        err_detail = tb.format_exc()
        cond_load_pymysql_params = f"Error During Func: load_params, Filename: {file_path} Type: {type(load_pymysql_params_ex).__name__}, Error Message: {load_pymysql_params_ex}, Trace Back: {err_detail}>>>\n"
        return cond_load_pymysql_params, None
##======>>>3. LOAD IN PARAMS
##======>>>4. Selenium OPEN WEBSITE
def open_web_page(target_url: str) -> Tuple[str, Optional[webdriver.Chrome]]:
    cond_open_webpage = ""
    try:
        # 設定 Chrome 下載目錄
        download_dir = os.getcwd()

        chrome_options = Options()
        chrome_prefs = {
            "download.default_directory": download_dir,##自動下載到程式路徑
            "download.prompt_for_download": False,     ##不要詢問下載
            "download.directory_upgrade": True,        ##自動建立目錄
            "safebrowsing.enabled": True               ##關閉安全提示
        }
        chrome_options.add_experimental_option("prefs", chrome_prefs)

        ##設定Chrome參數
        ##chrome_options = Options()
        chrome_options.add_argument("--headless")  # <<<< 重點: 無頭模式
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        ##chrome_options.add_argument("--no-sandbox")  ##關閉 sandbox
        chrome_options.add_argument("--disable-dev-shm-usage")  ##避免共享記憶體造成崩潰
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--log-level=3")

        driver = webdriver.Chrome(options=chrome_options)
        ##開啟目標網頁
        driver.get(target_url)

        ##發現需要等候網頁Load in
        time.sleep(5)
        exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        cond_open_webpage = f"Success opened Consumer webpage at: {exec_time}###\n"
        return cond_open_webpage, driver

    except Exception as open_web_ex:
        err_detail = tb.format_exc()
        cond_open_webpage = f"Error During Func: open_population_migration_page, Error: {open_web_ex}, Traceback: {err_detail}>>>\n"
        return cond_open_webpage, None
# 呼叫開啟網頁的 function
##======>>>4. Selenium OPEN WEBSITE
##======>>>5. Download and Rename
def download_file(driver) -> Tuple[str, Optional[str]]:
    cond_download_file = ""
    try:
        ##取得目前程式路徑
        download_dir = os.getcwd()
        cond_download_file += f"Download Path: {download_dir}###\n"

        ##點擊下載連結 (假設找到 XLSX 的 <a> 標籤)
        download_link = driver.find_element(By.CSS_SELECTOR, "a.xls")  # 或者 By.LINK_TEXT("XLSX")
        download_link.click()
        ##等待檔案下載完成
        time.sleep(5)
        ##找到最新下載的檔案
        downloaded_file = max(
            [os.path.join(download_dir, f) for f in os.listdir(download_dir) if f.endswith(".xlsx")],key=os.path.getctime)

        ##重新命名檔案
        new_name = f"CONSUMER_{datetime.now().strftime('%Y%m%d_%H_%M')}.xlsx"
        new_path = os.path.join(download_dir, new_name)
        shutil.move(downloaded_file, new_path)

        cond_download_file += f"Success downloaded and renamed file to {new_name}###\n"
        return cond_download_file, new_name

    except Exception as ex_download_file:
        err_detail = tb.format_exc()
        cond_download_file = f"Error During Func: download_file, Type: {type(ex_download_file).__name__}, Error: {ex_download_file}, Traceback: {err_detail}>>>\n"
        return cond_download_file, None
##======>>>
def py_mysql_connect(HOST, PORT, USER, PASSWORD, DB_NAME) -> Tuple[str, Optional[pymysql.connections.Connection]]:
    """Establish MySQL Connection, Return Conn"""
    condition_py_mysql_connect = ''
    try:
        conn = pymysql.connect(host=HOST,port=PORT,user=USER,password=PASSWORD, database=DB_NAME,cursorclass=pymysql.cursors.DictCursor)
        condition_py_mysql_connect = f"py_mysql_connect Successful Connected, DB Name: {conn.db.decode()}###\n"
    except Exception as py_mysql_connect_ex:
        condition_py_mysql_connect = f"py_mysql_connect Error: {py_mysql_connect_ex}>>>\n"
        conn = None##將Connection設定成None，這樣子即便連線出錯亦可以有2個物件回傳
    return condition_py_mysql_connect, conn
##======>>>
def py_mysql_insert(conn, data: pd.DataFrame, table_name: str) -> str:
    """Insert Data Into MySQL Table According to Table Name"""
    cond_py_mysql_insert = ""
    try:
        with conn.cursor() as cursor:
            delete_sql = f"DELETE FROM {table_name};"
            deleted_rows = cursor.execute(delete_sql)##取得刪除的筆數
            conn.commit()
            ##動態生成 INSERT SQL（不指定欄位）
            placeholders = ", ".join(["%s"] * len(data.columns))  ##依照 DataFrame 欄位數量產生 placeholder
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            print(f"This is Place Holders: {placeholders}###")
            insert_count = 0
            for idx, row in data.iterrows():
                cursor.execute(insert_sql, tuple(row))##Must Convert into Tuple
                print(f"This is Current Row: {row}###")
                insert_count = idx + 1
                conn.commit()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]
            cond_py_mysql_insert = f"Success Insert into {table_name}, delete old data: {deleted_rows} rows, re-insert: {insert_count} rows, complete at: {end_time}###\n"
            return cond_py_mysql_insert
    except Exception as py_mysql_insert_ex:
        conn.rollback()
        err_detail = tb.format_exc()
        cond_py_mysql_insert = f"Error During Func: py_mysql_insert, Type: {type(py_mysql_insert_ex).__name__}, Error Message: {py_mysql_insert_ex}, Trace Back: {err_detail}>>>\n"
        return cond_py_mysql_insert
##======>>>
##======>>>
if __name__ == "__main__":
    ##LOG BEGIN TIME
    program_beg_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
    program_begin_log = f"Program: Py_Consumer_Loan_&_Construction_loan.py, begins at: {program_beg_time}###\n"
    log_record(program_begin_log)
    ##DB Connection
    condition_load_mysql_params, mysql_params = load_params("py_mysql_15_index.csv")
    host = mysql_params.get("host")
    port = int(mysql_params.get("port"))
    user = mysql_params.get("user")
    password = mysql_params.get("password")
    db_name = mysql_params.get("db_name")
    cond_py_mysql_connect, mysql_conn = py_mysql_connect(host, port, user, password, db_name)
    log_record(cond_py_mysql_connect)
    
    ##Open Web Page
    condition_open_search_driver, get_info_driver = open_web_page(target_url="https://www.cbc.gov.tw/tw/cp-526-1078-7BD41-1.html")
    log_record(condition_open_search_driver)
    ##Download & Rename File
    if get_info_driver:
        condition_download_file, new_file_name = download_file(get_info_driver)
        log_record(condition_download_file)
    get_info_driver.close()
    ##pandas data manipulation
    ##dependency 'openpyxl'.  Use pip or conda to install openpyxl.
    ##pip install openpyxl
    df = pd.read_excel(new_file_name, header=None)
    ##1️找到0欄位是11001的索引
    start_idx = df[df[0] == "10901"].index[0]##要計算110-1月年增率
    ##2️從11001列開始取資料
    df_filtered = df.loc[start_idx:]
    ##3️建立consumers_purchasing (0~7 欄)
    consumers_purchasing = df_filtered.iloc[:, 0:8].reset_index(drop=True)
    ##4️建立construction_loan (0, 8 欄)
    construction_loan = df_filtered[[0, 8]].reset_index(drop=True)
    consumers_purchasing.columns = [
    "roc_year_month","consumer_total_sum","house_loan","house_repair_loan","car_loan","employee_welfare_loan","individual_consumption_loan","credit_card_loan"
    ]
    construction_loan.columns = [
    "roc_year_month","construction_loan"
    ]
    ##中華民國年/月拆分
    consumers_purchasing["roc_year"] = consumers_purchasing["roc_year_month"].astype(str).str[:3].astype(int)
    consumers_purchasing["roc_month"] = consumers_purchasing["roc_year_month"].astype(str).str[3:].astype(int)

    construction_loan["roc_year"] = construction_loan["roc_year_month"].astype(str).str[:3].astype(int)
    construction_loan["roc_month"] = construction_loan["roc_year_month"].astype(str).str[3:].astype(int)
    ##西元年/月建立
    consumers_purchasing["ad_year"] = consumers_purchasing["roc_year"] + 1911
    consumers_purchasing["ad_month"] = consumers_purchasing["roc_month"]

    construction_loan["ad_year"] = construction_loan["roc_year"] + 1911
    construction_loan["ad_month"] = construction_loan["roc_month"]
    ##移除原始年/月
    consumers_purchasing = consumers_purchasing.drop(columns=["roc_year_month"])
    construction_loan = construction_loan.drop(columns=["roc_year_month"])
    ##重新排序欄位
    consumers_purchasing = consumers_purchasing[
        ["roc_year", "roc_month", "ad_year", "ad_month", "consumer_total_sum", "house_loan", "house_repair_loan", "car_loan","employee_welfare_loan", "individual_consumption_loan", "credit_card_loan"]]
    construction_loan = construction_loan[["roc_year", "roc_month", "ad_year", "ad_month", "construction_loan"]]
    ##UPDATE_BY/UPDATE_TIME
    consumers_purchasing["update_by"] = "Py_Consumer_Loan_&_Construction_Loan.py"
    consumers_purchasing["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]

    construction_loan["update_by"] = "Py_Consumer_Loan_&_Construction_Loan.py"
    construction_loan["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]
    ##匯入資料庫: CONSUMERS_PURCHASING_STAGE & CONSTRUCTION_LOAN_STAGE
    cond_insert_consumers_purchasing = py_mysql_insert(mysql_conn, consumers_purchasing, "CONSUMERS_PURCHASING_STAGE")

    cond_insert_construction_loan = py_mysql_insert(mysql_conn, construction_loan, "CONSTRUCTION_LOAN_STAGE")

    log_record(cond_insert_consumers_purchasing)
    log_record(cond_insert_construction_loan)

    program_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]
    end_program = f"Program: Py_Consumer_Loan_&_Construction_Loan.py finished at: {program_end_time}###"
    log_record(end_program)
    if mysql_conn:
        mysql_conn.close()