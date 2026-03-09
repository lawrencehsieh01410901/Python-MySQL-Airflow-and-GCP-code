##2025-07-13 18:56 Begin
##1.透過匯入檔案參數決定起始年 v
##2.打開網頁: 遷入、遷出、淨遷徙人數按性別分(https://gis.ris.gov.tw/dashboard.html?key=E01)
##3.獲取查詢時間區間
##4.將查詢設定進行勾選
##5.對頁面資料進行查詢
##6.
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import time
import pymysql
##======>>>1. IMPORT
##======>>>2. LOG FUNCTION
LOG_FILE_NAME = f"Py_Popu_Migr_Scrapy_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}_LOG.txt"
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
def open_population_migration_page(target_url: str) -> Tuple[str, Optional[webdriver.Chrome]]:
    cond_open_webpage = ""
    try:
        ##設定Chrome參數
        chrome_options = Options()

        chrome_options.add_argument("--headless")  # <<<< 重點: 無頭模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--log-level=3")

        driver = webdriver.Chrome(options=chrome_options)
        ##開啟目標網頁
        driver.get(target_url)

        ##等待網頁載入到下拉選單OK
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select.input-sm.mb-md"))
        )
        ##發現需要等候網頁Load in
        time.sleep(5)
        exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        cond_open_webpage = f"Success opened population migration webpage at: {exec_time}###\n"
        return cond_open_webpage, driver

    except Exception as open_web_ex:
        err_detail = tb.format_exc()
        cond_open_webpage = f"Error During Func: open_population_migration_page, Error: {open_web_ex}, Traceback: {err_detail}>>>\n"
        return cond_open_webpage, None
# 呼叫開啟網頁的 function
##======>>>4. Selenium OPEN WEBSITE
##======>>>5. Selenium LOOP THROUGH ROC_BRGIN_YEAR, ALL MONTHS ~ CURRENT_YEAR, ALL MONTHS
def get_YM_list(get_YM_driver: webdriver.Chrome) -> Tuple[str, Optional[Dict[int, Dict[str, str]]]]:
    id = 1
    roc_year_month = dict()
    cond_get_YM_list =""
    try:
        for roc_year in range(int(roc_begin_year), int(roc_current_year) + 1):
            ##每次都重新抓一次年份下拉選單，不然會造成Selenium渲染後無效化
            roc_year_select = Select(get_YM_driver.find_element(By.XPATH, "(//select[@class='input-sm mb-md'])[1]"))##精準抓到"年"下拉選單
            valid_year_values = [opt.text.strip() for opt in roc_year_select.options]
            if str(roc_year) in valid_year_values:
                roc_year_select.select_by_visible_text(str(roc_year))
                
                roc_month_select = Select(get_YM_driver.find_element(By.XPATH, "(//select[@class='input-sm mb-md'])[2]"))##精準抓到"月"下拉選單
                valid_month_values = [ opt.text for opt in roc_month_select.options]
                for roc_month in valid_month_values:
                    if roc_month == "全年":
                        continue
                    else:
                        roc_year_month[id] = {"roc_year": str(roc_year), "roc_month": roc_month}
                        id += 1
                time.sleep(1)
        exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        cond_get_YM_list = f"Success Get ROC Year, Month List, Total Length: {len(roc_year_month)}, at: {exec_time}###\n"
        return cond_get_YM_list, roc_year_month
    except Exception as get_YM_list_ex:
        err_detail = tb.format_exc()
        cond_get_YM_list = f"Error During Func: get_YM_list, Type:{type(get_YM_list_ex).__name__}, Error Message: {get_YM_list_ex}, Trace Back: {err_detail}>>>\n"
        return cond_get_YM_list, None
##======>>>5. Selenium LOOP THROUGH ROC_BRGIN_YEAR, ALL MONTHS ~ CURRENT_YEAR, ALL MONTHS
##======>>>6. USE roc_year_month_dict INFO GO THRIUGH PAGES AND GET DATA

def set_search_bar(driver: webdriver.Chrome, search_year: str, search_month: str, check_column_name: str) -> str:
    try:
        cond_set_search_bar = ""
        ##下拉選單
        roc_getdata_year = Select(driver.find_element(By.XPATH, "(//select[@class='input-sm mb-md'])[1]"))
        roc_getdata_month = Select(driver.find_element(By.XPATH, "(//select[@class='input-sm mb-md'])[2]"))
        ##送入查詢年、月
        roc_getdata_year.select_by_visible_text(str(search_year))
        roc_getdata_month.select_by_visible_text(str(search_month))

        ##按照傳入名稱進行勾選: 遷入人數(check_move_in)、遷出人數(check_move_out)、淨遷徙人數(check_net_migration)
        if check_column_name == "check_move_in":
            check_move_in = driver.find_element(By.XPATH, "//input[@value='0_1']")
            if not check_move_in.is_selected():
                check_move_in.click()
        if check_column_name == "check_move_out":
            check_move_out = driver.find_element(By.XPATH, "//input[@value='0_2']")
            if not check_move_out.is_selected():
                check_move_out.click()
        if check_column_name == "check_net_migration":
            check_net_migration = driver.find_element(By.XPATH, "//input[@value='0_3']")
            if not check_net_migration.is_selected():
                check_net_migration.click()

        ##勾選男、女選項
        check_male = driver.find_element(By.XPATH, "//input[@value='1_4']") ##//input[@value='1_4']
        check_female = driver.find_element(By.XPATH, "//input[@value='1_5']")

        if not check_male.is_selected():
            check_male.click()
        if not check_female.is_selected():
            check_female.click()

        search_button = get_info_driver.find_element(By.XPATH, "//button[contains(text(),'查詢')]")
        search_button.click()

        exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        cond_set_search_bar = f"Searched:{search_year}-{search_month}-{check_column_name} Success at: {exec_time}###\n"
        return cond_set_search_bar
    except Exception as get_move_in_ex:
        err_detail = tb.format_exc()
        cond_set_search_bar = f"Error During Func: set_search_bar at: {search_year}-{search_month}-{check_column_name}, Type:{type(get_move_in_ex).__name__}, Error Message: {get_move_in_ex}, Track Back: {err_detail}>>>\n"
##======>>>6. USE roc_year_month_dict INFO GO THRIUGH PAGES AND GET DATA
##======>>>7. Scrape Data
def extract_table_rows(driver: webdriver.Chrome, search_year: str, search_month: str, check_column_name: str, all_page_data: dict = None) -> Tuple[str, Optional[Dict[int, Dict[str, str]]]]:
    """Get Each Check Box's Data and """
    cond_extract_table_rows = ""
    time.sleep(3)##等資料載入
    ##每一次透過指定: check_column_name = check_move_in就可以將資料重新歸0，重新開始
    if all_page_data is None:
        all_page_data = {}
    id = 1
    try:
        while True:
            rows = driver.find_elements(By.XPATH, "//table[@id='datatable']/tbody/tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 3 and check_column_name == "check_move_in":
                    city = cols[0].text.strip().replace(" ", "")
                    m_move_in = cols[1].text.strip()
                    f_move_in = cols[2].text.strip()

                    all_page_data[id] = {
                        "roc_year": search_year,
                        "roc_month": search_month,
                        "city": city,
                        "m_move_in": m_move_in,
                        "f_move_in": f_move_in
                    }
                    id += 1
                elif len(cols) >= 3 and check_column_name == "check_move_out":
                    ##city = cols[0].text.strip()
                    m_move_in = cols[1].text.strip()
                    f_move_in = cols[2].text.strip()

                    all_page_data[id].update({
                        "m_move_out": m_move_in,
                        "f_move_out": f_move_in
                    })
                    id += 1##一樣要透過id += 1去掃過整個dict{ID編號}
                elif len(cols) >= 3 and check_column_name == "check_net_migration":
                    ##city = cols[0].text.strip()
                    m_net_migration = cols[1].text.strip()
                    f_net_migration = cols[2].text.strip()

                    all_page_data[id].update({
                        "m_net_migration": m_net_migration,
                        "f_net_migration": f_net_migration,
                        "create_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2],###
                        "create_by": "Py_Popu_Migr_Scrapy.py"
                    })
                    id += 1##一樣要透過id += 1去掃過整個dict{ID編號}
            ##檢查下一頁按鈕是否可點擊(class = 'paginate_button next'）
            try:
                next_li = driver.find_element(By.XPATH, "//li[contains(@class, 'next')]")
                if "disabled" in next_li.get_attribute("class"):
                    break  # 已無下一頁
                else:
                    next_button = next_li.find_element(By.TAG_NAME, "a")
                    next_button.click()
                    time.sleep(1)
            except Exception as next_page_ex:
                err_detail = tb.format_exc()
                cond_extract_table_rows = f"Error During extract_table_rows-next_page, Type:{type(next_page_ex).__name__}, Error Message: {next_page_ex}, Trace Back:{err_detail}>>>\n"
                break
        exec_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        cond_extract_table_rows = f"Get {search_year}-{search_month}-{check_column_name}, len: {len(all_page_data)} Success at: {exec_time}###\n"
        return cond_extract_table_rows, all_page_data
    except Exception as extract_table_rows_ex:
        err_detail = tb.format_exc()
        cond_extract_table_rows = f"Error During extract_table_rows, Type:{type(extract_table_rows_ex).__name__}, Error Message: {extract_table_rows_ex}, Trace Back:{err_detail}>>>\n"
        return all_page_data, None
##======>>>7. Scrape Data
##======>>>8. After Each Search Year - Month Scraped: Insert into MySQL
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
def mysql_insert_transaction(conn, data_dict: dict[int, dict], table_name: str, roc_year: str, roc_month: str,) -> str:
    """Row-by-row insert，失敗的列會被捕捉並寫入log，不影響其他列"""
    condition_insert = ""
    success_cnt = 0
    fail_logs: list[str] = []
    try:
        with conn.cursor() as cursor:
            ##1.刪除相同舊資料
            delete_sql = f"DELETE FROM {table_name} WHERE ROC_YEAR = '{roc_year}' AND ROC_MONTH = '{roc_month}';"
            cursor.execute(delete_sql)
            conn.commit()##確定刪除生效
            ##2.單筆INSERT語句(11 Columns)
            insert_sql = f"""
                INSERT INTO {table_name} (
                EACH_YEAR_MONTH_CITY_DISTRICT_ID, ROC_YEAR, ROC_MONTH, CITY_DISTRICT_NAME, M_MOVE_IN, F_MOVE_IN, M_MOVE_OUT, F_MOVE_OUT, M_NET_MIGRATION, F_NET_MIGRATION, CREATE_TIME, CREATE_BY
                ) 
                VALUES (%s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s
                )
            """
            ##3. 逐筆匯入
            for cid, rec in data_dict.items():
                row = rec.copy()
                row["each_year_month_city_district_id"] = cid
                try:
                    values = (
                    cid,  # <== 用 dict 的 key 作為 ID
                    row.get("roc_year"),
                    row.get("roc_month"),
                    row.get("city").replace(" ", ""),  # 去除中間空白
                    row.get("m_move_in"),
                    row.get("f_move_in"),
                    row.get("m_move_out"),
                    row.get("f_move_out"),
                    row.get("m_net_migration"),
                    row.get("f_net_migration"),
                    row.get("create_time"),
                    row.get("create_by")
                    )
                    cursor.execute(insert_sql, values)
                    success_cnt += 1
                except Exception as e:
                    ##收集失敗列的關鍵識別資訊
                    fail_logs.append(f"Insert Error at id:{cid}-{roc_year}-{roc_month}, Error Message: {e}>>>\n")
                    ##不rollback，直接跳下一筆
            conn.commit()##一次性提交所有成功的列
        # 組合回傳訊息
        if fail_logs:
            log_parts = []
            for i, msg in enumerate(fail_logs, start=1):
                log_parts.append(f"{msg}MySQL Insert 完成：成功 {success_cnt} 筆，失敗 {i} 筆###\n")
            condition_insert = "".join(log_parts)
            ##condition_insert = (f"MySQL Insert 完成：成功 {success_cnt} 筆，失敗 {len(fail_logs)} 筆###\n".join(fail_logs) + "###\n")
        else:
            condition_insert = (f"MySQL Insert Success: {roc_year}-{roc_month}:"f"{success_cnt} rows inserted.###\n")
    except Exception as insert_ex:
        # 重大錯誤才全體 rollback
        conn.rollback()##HERE!!!
        condition_insert = f"MySQL Insert Error (fatal): {insert_ex}>>>\n"
    return condition_insert
##======>>>8. After Each Search Year - Month Scraped: Insert into MySQL
##======>>>main()
if __name__ == "__main__":
    try:
        ##LOG BEGIN TIME
        program_beg_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        program_begin_log = f"Program: Py_Popu_Migr_Scrapy.py, begins at: {program_beg_time}###\n"
        log_record(program_begin_log)

        ##======>>>ESTABLISH YEAR DURATION & DB Connect
        condition_load_search_params, popu_migr_params = load_params("py_population_migration_params.csv")
        roc_begin_year = popu_migr_params.get("roc_begin_year")
        roc_current_year = str(datetime.now().year - 1911)
        target_url = popu_migr_params.get("target_url")
        log_record(condition_load_search_params)

        condition_load_mysql_params, mysql_params = load_params("py_mysql_tw_population_migration_params.csv")
        host = mysql_params.get("host")
        port = int(mysql_params.get("port"))
        user = mysql_params.get("user")
        password = mysql_params.get("password")
        db_name = mysql_params.get("db_name")
        cond_py_mysql_connect, mysql_conn = py_mysql_connect(host, port, user, password, db_name)
        log_record(cond_py_mysql_connect)
        ##======>>>ESTABLISH YEAR DURATION & DB Connect
        ##LOAD IN SEARCH WEBSITE PARAMS
        condition_get_YM_browser, get_YM_driver = open_population_migration_page(target_url=target_url)
        log_record(condition_get_YM_browser)
        ##GET ROC YEAR-MONTH DURATION
        condition_get_YM, roc_year_month_dict = get_YM_list(get_YM_driver)
        log_record(condition_get_YM)
        get_YM_driver.close()
        ##RE-OPEN & BEGIN SEARCH
        condition_open_search_driver, get_info_driver = open_population_migration_page(target_url=target_url)
        log_record(condition_open_search_driver)
        for data in range(1, len(roc_year_month_dict) + 1): 
            print(f"Search: {roc_year_month_dict[data]['roc_year']}-{roc_year_month_dict[data]['roc_month']} Begin###")
            ##SET SEARCH BAR: MOVE IN DATA SEARCH
            condition_search_move_in = set_search_bar(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], check_column_name="check_move_in")
            log_record(condition_search_move_in)
            ##SCRAPE MOVE IN DATA
            condition_scrape_move_in, each_page_data = extract_table_rows(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], "check_move_in")
            log_record(condition_scrape_move_in)
            ##SET SEARCH BAR: MOVE OUT DATA SEARCH
            condition_search_move_out = set_search_bar(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], check_column_name="check_move_out")
            log_record(condition_search_move_out)
            ##SCRAPE MOVE OUT DATA
            condition_scrape_move_out, each_page_data = extract_table_rows(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], check_column_name="check_move_out", all_page_data = each_page_data)
            log_record(condition_scrape_move_out)
            ##SET SEARCH BAR: NET MIGRATION DATA SEARCH
            condition_search_net_migration = set_search_bar(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], check_column_name="check_net_migration")
            log_record(condition_search_net_migration)
            ####SCRAPE NET MIGRATION DATA
            condition_scrape_net_migr, each_page_data = extract_table_rows(get_info_driver, roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'], check_column_name="check_net_migration", all_page_data = each_page_data)
            log_record(condition_scrape_net_migr)
            condition_insert_data = mysql_insert_transaction(mysql_conn, each_page_data, "INTERIOR_WEB_POPULATION_MIGRATION_STAGE", roc_year_month_dict[data]['roc_year'],roc_year_month_dict[data]['roc_month'])
            log_record(condition_insert_data)
            print(f"Search: {roc_year_month_dict[data]['roc_year']}-{roc_year_month_dict[data]['roc_month']} Ends###")
        
        get_info_driver.close()
        program_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        program_end_log = f"Program: Py_Popu_Migr_Scrapy.py, Ends at: {program_end_time}###\n"
        log_record(program_end_log)
    except Exception as main_ex:
        err_detail = tb.format_exc()
        main_cond = f"Error During main(), Type:{type(main_ex).__name__}, Error Message: {main_ex}, Trace Back: {err_detail}>>>\n"
##======>>>main()
