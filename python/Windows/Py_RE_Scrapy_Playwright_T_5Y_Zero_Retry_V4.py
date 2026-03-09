##======>>>1. IMPORT
import asyncio
from playwright.async_api import async_playwright, Browser, Frame
from datetime import datetime
import csv
from typing import Tuple, Optional, Dict, Any
from pathlib import Path
import pymysql
import traceback as tb
import re, math
import sys ## 2025-08-17重大更新
##======>>>1. IMPORT
LOG_FILE_NAME = f"INTERIOR_WEB_RE_TRANSACTION_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}_LOG_FULL.txt"##2025-08-12 HOTFIX，用以跟D_M的LOG做區分
LOG_FILE_PATH = Path(LOG_FILE_NAME)
##======>>>2. LOG FUNCTION
async def log_record(msg: str):
    """Write Each State's Program Logs, Return Nothing"""
    try:
        with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
            f.write(msg)
    except Exception as e:
        print(f"log_record 寫入失敗: {e}")
##======>>>2. LOG FUNCTION
##======>>>3. DATABASE CONNECTION & SEARCH BAR PARAMS
##Establish Program Begin Time
program_beg_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
program_begin_log = f"Program: Py_Real_Estate_Scrapy.py, begins at: {program_beg_time}###\n"
##Load In MySQL Connection Params
##Change: 2025-07-09 Load Both CSV
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
            cond_load_pymysql_params = f"pymysql_params Successful Loaded###\n"
        return cond_load_pymysql_params, param_dict
    except Exception as load_pymysql_params_ex:
        err_detail = tb.format_exc()
        cond_load_pymysql_params = f"Error During Func: load_pymysql_params, Type: {type(load_pymysql_params_ex).__name__}, Error Message: {load_pymysql_params_ex}, Trace Back: {err_detail}>>>\n"
        return cond_load_pymysql_params, None

cond_load_pymysql_params, mysql_params = load_params("py_mysql_tw_real_estate_params.csv")
program_begin_log += cond_load_pymysql_params
##Get Params for DB Connection
host = mysql_params.get('host')
port = int(mysql_params.get('port'))
user = mysql_params.get('user')
password = mysql_params.get('password')
db_name = mysql_params.get('db_name')
##2025-07-09 Load in Search Bar: House, Land, Establishment, Parking
cond_load_searchbar_params, searchbar_params = load_params("py_real_estate_search_bar_params.csv")
customCheck1 = (searchbar_params.get("customCheck1") == "True")
customCheck2 = (searchbar_params.get("customCheck2") == "True")
customCheck3 = (searchbar_params.get("customCheck3") == "True")
customCheck4 = (searchbar_params.get("customCheck4") == "True")
previous_years = int(searchbar_params.get("previous_years"))

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
cond_py_mysql_connect, mysql_conn = py_mysql_connect(host, port, user, password, db_name)
program_begin_log += cond_py_mysql_connect
##======>>>3. DATABASE CONNECTION & SEARCH BAR PARAMS
##======>>>4. async OPEN WEBSITE
async def async_open_website(async_p, url: str, wait_open_time: int = 5000) -> Tuple[str, Optional[Browser], Optional[Frame]]:
    """Open Website in Playwright Async Mode, Default Wait Time: 5 sec, Return Browser & Frame as Driver"""
    cond_async_open_website = ""
    open_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
    try:                      ##Here, Assign Chromium as our Browser
        browser = await async_p.chromium.launch(headless=True,
                                                args=["--start-maximized", "--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        )
        page = await ctx.new_page()
        await page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        await page.goto(url)
        await page.wait_for_timeout(wait_open_time)
        await page.wait_for_selector("frame")
        frame_element = await page.query_selector("frame")
        driver = await frame_element.content_frame()##Here, Use Frame as Driver to Handel/Control the Webpage's Movement
        cond_async_open_website = f"Async Open Success, time: {open_time}###\n"
    except Exception as async_open_website_ex:
        err_detail = tb.format_exc()
        cond_async_open_website = f"Error During Func: async_open_website, Type: {type(async_open_website_ex).__name__}, Error Message: {async_open_website_ex}, Trace Back: {err_detail}>>>\n"
    return cond_async_open_website, browser, driver
##======>>>4. async OPEN WEBSITE
##======>>>5. Get All City/District
async def get_city_district_list(browser: Browser, driver: Frame) -> Dict[int, Dict[str, str]]:
    """Get All TW's City/District Names, Return in Dict(id: {"city_name": data, "district_name": data})"""
    cond_get_city_district_list = ""
    city_district_dict = dict()
    try:
        cities_list = []
        ##Get City/District Dropdown List as Data
        city_options = await driver.query_selector_all("#p_city option")

        for option in city_options:
            city = (await option.inner_text()).strip()
            if city != "縣市":
                cities_list.append(city)
        int_id = 1
        for city in cities_list:
            await driver.select_option("#p_city", label=city)
            await driver.wait_for_timeout(500)
            await driver.wait_for_function(
                "() => document.querySelectorAll(\"#p_town option\").length > 1"
            )
            district_options = await driver.query_selector_all("#p_town option")
            for option in district_options:
                district = (await option.inner_text()).strip()
                if district != "請選擇":
                    city_district_dict[int_id] = {"city": city, "district": district}
                    int_id += 1
        await driver.wait_for_timeout(1000)
        await browser.close()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cond_get_city_district_list = f"TW Citys and Districts Success, total rows: {len(city_district_dict)}, finished time: {now}###\n"
    except Exception as get_city_district_list_ex:
        err_detail = tb.format_exc()
        cond_get_city_district_list = f"Error During Func: get_city_district_list, Type: {type(get_city_district_list_ex).__name__}, Error Message: {get_city_district_list_ex}, Trace Back: {err_detail}>>>\n"
    return cond_get_city_district_list, city_district_dict
##======>>>5. Get All City/District
##======>>>6. Prepare Search Bar
async def prepare_search_bar(browser: Browser, driver: Frame, city_name: str, district_name: str, customCheck1: bool, customCheck2: bool, customCheck3: bool, customCheck4: bool, previous_years: int, wait_sec: int = 5000) -> str:
    """Prepare The Search Bar According to the Dropdown List & Date Duration, etc"""
    cond_prepare_search_bar = ""
    try:
        ##---------- 1.縣市設定----------
        await driver.select_option("#p_city", label=city_name)
        await driver.wait_for_timeout(1000)
        ##---------- 2.等鄉鎮市區選單 AJAX 載入----------
        await driver.wait_for_function("document.querySelector('#p_town')?.disabled === false")
        await driver.select_option("#p_town", label=district_name)
        await driver.wait_for_timeout(1000)
        ##---------- 3.勾選建物、土地、車位----------
        if customCheck1 == True:
            checkbox = await driver.query_selector('input#customCheck1')
            if checkbox is not None:
                is_checked = await checkbox.is_checked()
                if not is_checked:
                    await checkbox.check()
        if customCheck2 == True:
            label_2 = await driver.wait_for_selector('label[for="customCheck2"]')
            await label_2.click()
        if customCheck3 == True:
            label_3 = await driver.wait_for_selector('label[for="customCheck3"]')
            await label_3.click()
        if customCheck4 == True:
            label_4 = await driver.wait_for_selector('label[for="customCheck4"]')
            await label_4.click()
        ##---------- 4.設定最小年、月，最大年、月----------
        beg_year = max([int(await (await opt.get_property("textContent")).json_value())
                       for opt in await driver.query_selector_all("#p_endY option") if await opt.get_attribute("value")]) - previous_years
        beg_month = min([int(await (await opt.get_property("textContent")).json_value())
                        for opt in await driver.query_selector_all("#p_startM option") if await opt.get_attribute("value")])
        end_year = max([int(await (await opt.get_property("textContent")).json_value())
                       for opt in await driver.query_selector_all("#p_endY option") if await opt.get_attribute("value")])
        end_current_month = datetime.now().month

        await driver.select_option("#p_startY", label=str(beg_year))
        await driver.select_option("#p_startM", label=str(beg_month))
        await driver.select_option("#p_endY", label=str(end_year))
        await driver.select_option("#p_endM", label=str(end_current_month))
        await driver.wait_for_timeout(1000)
        ##----------5.單價設定為元(精準值)----------
        unit_price = await driver.query_selector('input.form-check-input.font[value="2"]')
        if unit_price:
            await unit_price.click()
        await driver.wait_for_timeout(1000)
        ##----------6.點擊搜尋按鈕----------
        search_button = await driver.query_selector('a.btn.btn-a.form-button[go_type="list"]')
        if search_button:
            await driver.evaluate(
                '''(btn) => {
                    const event = new MouseEvent("click", {
                        bubbles: true,
                        cancelable: true,
                        view: window
                    });
                    btn.dispatchEvent(event);
                }''',
                search_button
            )
            await driver.wait_for_timeout(wait_sec)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cond_prepare_search_bar = f"Prepare Search Bar Success and Searched: {city_name}-{district_name}, at: {current_time}###\n"
    except Exception as prepare_search_bar_ex:
        err_detail = tb.format_exc()
        cond_prepare_search_bar = f"Error During Func: prepare_search_bar, Type:{type(prepare_search_bar_ex).__name__}, Error Message: {prepare_search_bar_ex}, Trace Back: {err_detail}>>>\n"
    return cond_prepare_search_bar
##======>>>6. Prepare Search Bar
##======>>>7. Get Current City/District's Last Page
async def get_last_page_num(browser: Browser, driver: Frame) -> Tuple[str, int]:
    """Get Current City/District's Last Page Number"""
    cond_get_last_page = ""
    last_page_number = None
    try:  ##Since Time Duration is Min ~ Current, Need Time to Loading
        await driver.wait_for_selector("#price_table_info", timeout=100000)##100 seconds before time out
        ##======>>>
        ##Wait Until Left-Down is Not Empty
        max_retry = 5##Retry 5 Times, Total Timeout: 150秒
        for i in range(max_retry):
            info_elem = await driver.query_selector("#price_table_info")
            info_text = await info_elem.inner_text() if info_elem else ""
            
            if re.search(r'查詢結果\s*[:：]\s*([\d,]+)\s*筆', info_text):
                break  ##If Get, break
            else:
                await driver.wait_for_timeout(100000)##Wait 100 Secs 2025-07-09 hotfix!
        ##======>>>
        ##Parse Words into Last Page Number
        match = re.search(r'查詢結果\s*[:：]\s*([\d,]+)\s*筆', info_text)
        if match:
            total_records = int(match.group(1).replace(",", ""))
            last_page_number = math.ceil(total_records / 15)

            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cond_get_last_page = f"Get Last Page From Info Text: {total_records} rows, Total Pages: {last_page_number}, Time: {now}###\n"
            await browser.close()##After Getting Last Page, Close
        else:
            cond_get_last_page = f"Cannot extract total rows from text: '{info_text}'###\n"
    except Exception as get_last_page_num_ex:
        err_detail = tb.format_exc()
        cond_get_last_page = f"Error During Func: get_last_page_num, Type: {type(get_last_page_num_ex).__name__}, Error Message: {get_last_page_num_ex}, Trace Back: {err_detail}>>>\n"
    return cond_get_last_page, last_page_number
##======>>>7. Get Current City/District's Last Page
##======>>>8. According to Last Page Number, Establish Bacth
async def establish_batch(last_page_number: int, batch_size: int = 300) -> Tuple[str, Dict[int, Dict[str, int]]]:
    """According to Last Page Number(last_page_number), Establish Bacth Size, Return Dict"""
    ##2025-07-04紀錄:測試過後，頁面最好是不要超過10個，否則效能一樣緩慢
    cond_establish_batch = ""
    batch_dict = {}
    batch_id = 1
    if last_page_number >= 2000:
        batch_size = 500##整數，好做分區、分頁查看
    elif last_page_number >= 1000 and last_page_number < 2000:
        batch_size = 300
    elif last_page_number < 1000:
        batch_size = 100##乖啦~別折騰了，效能瓶頸問題
    try:
        for start in range(1, last_page_number + 1, batch_size):
            end = min(start + batch_size - 1, last_page_number)
            batch_dict[batch_id] = {
                "batch_id": batch_id,
                "begin_page_num": start,
                "end_page_num": end
            }
            batch_id += 1
        cond_establish_batch = f"Created Batch Success: {len(batch_dict)}###\n"
    except Exception as establish_batch_ex:
        err_detail = tb.format_exc()
        cond_establish_batch = f"Error During Func: establish_batch, Type: {type(establish_batch_ex).__name__}, Error Message: {establish_batch_ex}, Trace Back: {err_detail}>>>\n"
    return cond_establish_batch, batch_dict
##======>>>8. According to Last Page Number, Establish Bacth
##======>>>9. Go to Each Batch's Begin Page and Next Page
async def goto_page(driver: Frame, page_num: int, wait_sec: float = 1) -> str:
    """跳轉至指定頁碼: driver:frame物件, page_num: 欲前往的頁碼, return: log 字串"""
    try:
        await driver.wait_for_timeout(wait_sec * 1000)
        ##1.輸入頁碼(fill會自動清空並輸入)
        await driver.fill("#p_page", str(page_num))##這邊也要改!
        await driver.wait_for_timeout(500)
        ##2.點擊「GO」按鈕（使用JS）
        max_retry = 5##Total 500秒
        for i in range(max_retry):
            go_click = await driver.query_selector('//a[text()="GO"]')
            if go_click:
                break
            else:
                await driver.wait_for_timeout(1000000)##100秒
        if go_click:
            await driver.evaluate('''(el) => {
                el.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }''', go_click)
        await driver.wait_for_timeout(wait_sec * 1000)
        return f"跳轉至:{page_num}頁###\n"
    except Exception as goto_page_e:
        return f"跳轉至:{page_num}頁失敗: {goto_page_e}>>>\n"
##======>>>9. Go to Each Batch's Begin Page and Next Page
##======>>>10. Scraped Each Current Page's Info
async def to_half_width(text):
    """將全形文字轉換為半形（含數字、字母、標點）"""
    return ''.join(chr(ord(char) - 65248) if 65281 <= ord(char) <= 65374 else ' ' if ord(char) == 12288 else char
        for char in text)

async def scrap_each_page(driver: Frame, batch_id: int, each_page: int, city_name: str, district_name: str) -> tuple[dict, str]:
    """針對目前頁面抓資料，不包含翻頁功能"""
    each_city_district_data_id = 1
    cleans_city_district_data: dict[int, dict] = {}
    cond_scrap_each_page = ""
    try:
        await driver.wait_for_selector('#table-item-tbody tr')
        rows = await driver.query_selector_all('#table-item-tbody tr')
        each_city_district_data = {}

        for row in rows:
            tds = await row.query_selector_all('td')
            
            data_texts = [await to_half_width((await td.inner_text()).strip()) for td in tds]##要加 await to_half_width
            each_city_district_data[each_city_district_data_id] = data_texts
            each_city_district_data_id += 1
            ##======>>>
            price_td = tds[2]
            if await price_td.query_selector('span.btP_br'):
                data_texts[2] = data_texts[2] + '車'
            # --- ② 型態：由 icon class 轉成中文 ---
            type_td = tds[7]
            icon_span = await type_td.query_selector("span[class$='_br']")
            if icon_span:
                icon_cls = (await icon_span.get_attribute('class')).split()[0]   # e.g. bt06_br
                icon_key = icon_cls.split('_')[0]                                # bt06
                type_map = {
                    "bt01": "寓", "bt02": "透", "bt05": "樓", "bt06": "華",
                    ##2025-07-10 12:39 Add:
                    ##請檢查是否有: 主要用途(MAIN_PURPOSE)是住家用，但是型態(TYPE)是: "無"的
                    ##屆時，非住家用的都會被過濾掉!!!
                    "bt09": "廠", "bt03": "店", "bt07": "套"
                }
                data_texts[7] = type_map.get(icon_key, data_texts[7] or '無')
            # --- ③ 交易標的：去掉隱藏 span 與空白 ---
            target_td = tds[11]
            target_text = await target_td.evaluate(
                'el => [...el.childNodes]'
                '.filter(n => n.nodeType===3 || (n.nodeType===1 && getComputedStyle(n).display!="none"))'
                '.map(n => n.textContent)'
                '.join("")'
            )
            data_texts[11] = re.sub(r"\\s+", "", target_text) or '無'
            ##======>>>
        for key, value in each_city_district_data.items():
            cleans_city_district_data[key] = {
                "batch_id": batch_id,
                'page': each_page,
                "city_name": city_name,
                "district_name": district_name,
                ##======>>>去除多餘空白
                "address": (value[0].strip() if value[0] else '無'),
                "community": (value[1].strip() if value[1] else '無'),
                "total_price_10K": (value[2].strip() if value[2] else '無'),
                "transaction_date": (value[3].strip() if value[3] else '無'),
                "unit_price": (value[4].strip() if value[4] else '無'),
                "total_tsubo": (value[5].strip() if value[5] else '無'),
                "real_space_percent": (value[6].strip() if value[6] else '無'),
                "type": (value[7].strip() if value[7] else '無'),
                "house_age": (value[8].strip() if value[8] else '無'),
                "level": (value[9].strip() if value[9] else '無'),
                "main_purpose": (value[10].strip() if value[10] else '無'),
                "transaction_target": (value[11].strip() if value[11] else '無'),
                "building_structure": (value[12].strip() if value[12] else '無'),
                "car_parking_price_10K": (value[13].strip() if value[13] else '無'),
                "manage_unit_yn": (value[14].strip() if value[14] else '無'),
                "elevator_yn": (value[15].strip() if value[15] else '無'),
                "note": (value[18].strip() if value[18] else '無'),
                ##======>>>去除多餘空白
                "scraped_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
            }
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cond_scrap_each_page = f"{city_name}-{district_name}, p.{each_page} done at:{current_time}###\n"

    except Exception as scrap_each_page_ex:
        err_detail = tb.format_exc()
        cond_scrap_each_page = f"Error During Func: scrap_each_page(): Type: {type(scrap_each_page_ex).__name__}, Error Message: {scrap_each_page_ex}, Trace Back: {err_detail}>>>\n"
    return cond_scrap_each_page, cleans_city_district_data
##======>>>10. Scraped Each Current Page's Info
##======>>>11. Execute Each Batch: Every batch has: goto_page(), to_half_width(), scrap_each_page()
async def exec_batch(async_p, batch_info: dict, city: str, district: str) -> Tuple[str, Dict[int, Dict[str, Any]]]:
    """Each batch: 1.開瀏覽器 & prepare_search_bar, 2.依batch的page範圍循環goto_page(), 3.在迴圈內呼叫scrap_each_page()抓取資料, 4.回傳log & Total scraped data"""
    batch_id   = batch_info["batch_id"]
    start_page = batch_info["begin_page_num"]
    end_page   = batch_info["end_page_num"]
    per_batch_records = {}
    per_batch_id = 1

    logs = f"[Batch {batch_id}]: \n"
    try:
        ##1.開新瀏覽器並切換frame
        cond_open, browser, driver = await async_open_website(async_p, "https://lvr.land.moi.gov.tw/")
        logs += cond_open
        ##2.設定搜尋條件
        cond_prepare = await prepare_search_bar(browser, driver, city, district, customCheck1, customCheck2, customCheck3, customCheck4, previous_years)
        logs += cond_prepare
        dummy_logs = ""
        ##3.逐頁翻頁(視需要調整wait_sec)
        for page in range(start_page, end_page + 1):
            if page == start_page:
                ##發現開啟多個瀏覽器時，速度變慢，改成10秒20250704 10:24
                ##每一個獨立batch的第一頁，等候10秒鐘
                dummy_logs += await goto_page(driver, page, wait_sec=10)
            else:
                dummy_logs += await goto_page(driver, page, wait_sec=1)
            if page == end_page:##都故意只取最後一筆的log
                logs += await goto_page(driver, page, wait_sec=1)
            ##4.爬取資料
            cond_scrap_each_page, records = await scrap_each_page(driver, batch_id, page, city, district)
            if page == end_page:##都故意只取最後一筆的log
                logs += cond_scrap_each_page

            for v_records in records.values():
                per_batch_records[per_batch_id] = v_records
                per_batch_id += 1
        ##5.如該批次已執行完畢，關閉瀏覽器
        await browser.close()
    except Exception as exec_batch_ex:
        err_detail = tb.format_exc()
        logs += f"Error During Func: exec_batch, Type: {type(exec_batch_ex).__name__}, Error Message: {exec_batch_ex}, Trace Back: {err_detail}>>>\n"
    return logs, per_batch_records
##======>>>11. exec_batch
##======>>>12. Insert Into MySQL
async def my_sql_check_list_count(conn, table_name: str):
    cond_my_sql_check_list_count = ""
    count = 0  # 預設值，避免 UnboundLocalError
    try:
        with conn.cursor() as cursor:
            count_sql = f"SELECT COUNT(*) AS cnt FROM {table_name};"
            # print(f"[DEBUG] Executing SQL: {count_sql}")
            cursor.execute(count_sql)
            result = cursor.fetchone()
            count = result["cnt"]
            # print(f"[DEBUG] Result: {result}")
            if count == 0:
                cond_my_sql_check_list_count = f"First Time Run, with: {count} in Table, Begin Insert City/District###\n"
            else:
                cond_my_sql_check_list_count = f"NOT First Time Run, with: {count} Already in Table, Skip Insert City/District###\n"
    except Exception as ex_my_sql_check_list_count:
        cond_my_sql_check_list_count = f"MySQL Check Error (fatal): {ex_my_sql_check_list_count}>>>\n"

    return cond_my_sql_check_list_count, count

async def mysql_insert_check_list(conn, data_dict: dict[int, dict], table_name: str) -> str:
    """Row-by-row insert，建立縣市/鄉鎮區資料的資料開始時間
       If you want to do a Full Re-Run From the start, Please TRUNCATE TABLE: INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST"""
    cond_mysql_insert_check_list = ""
    success_cnt = 0
    fail_cnt = 0
    fail_logs: list[str] = []
    try:
        with conn.cursor() as cursor:
            insert_sql = f"""
                INSERT INTO {table_name}(RECORD_ID, CITY_NAME, DISTRICT_NAME, ESTABLISH_TIME)
                VALUES(%(id)s, %(city)s, %(district)s, %(established_time)s)"""
            
            for rid, rec in data_dict.items():
                row = rec.copy()
                row["id"] = rid
                row["established_time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
                try:
                    cursor.execute(insert_sql, row)
                    success_cnt += 1
                except Exception as e:
                    fail_cnt += 1
                    fail_logs.append(f"Insert Error at id:{rid}, city:{row['city']}, district:{row['district']} Error Message: {e}, count: {fail_cnt}>>>\n")
                conn.commit()##一次性提交所有成功的列
            # 組合回傳訊息
        if fail_logs:
            cond_mysql_insert_check_list = (f"MySQL Insert 完成：成功 {success_cnt} 筆，失敗 {len(fail_logs)} 筆###\n".join(fail_logs) + "###\n")
        else:
            cond_mysql_insert_check_list = (f"MySQL Insert Success: {table_name}:"f"{success_cnt} rows inserted.###\n")
    except Exception as ex_mysql_insert_check_list:
        # 重大錯誤才全體 rollback
        conn.rollback()##HERE!!!
        cond_mysql_insert_check_list = f"MySQL Insert Error (fatal): {ex_mysql_insert_check_list}>>>\n"
    return cond_mysql_insert_check_list

async def mysql_insert_transaction(conn, data_dict: dict[int, dict], table_name: str, city_name: str, district_name: str,) -> str:
    """Row-by-row insert，失敗的列會被捕捉並寫入log，不影響其他列"""
    condition_insert = ""
    success_cnt = 0
    fail_logs: list[str] = []
    try:
        with conn.cursor() as cursor:
            ##1.刪除相同舊資料
            delete_sql = f"DELETE FROM {table_name} WHERE CITY_NAME = '{city_name}' AND DISTRICT_NAME = '{district_name}';"
            cursor.execute(delete_sql)
            conn.commit()##確定刪除生效
            ##2.單筆INSERT語句(23 Columns)
            insert_sql = f"""
                INSERT INTO {table_name}(
                    city_district_id, batch_id, page, city_name, district_name, address, community, total_price_10K, transaction_date,
                    unit_price, total_tsubo, real_space_percent, type,
                    house_age, level, main_purpose, transaction_target,
                    building_structure, car_parking_price_10K, manage_unit_yn,
                    elevator_yn, note, scraped_time
                ) VALUES (
                    %(city_district_id)s, %(batch_id)s, %(page)s,
                    %(city_name)s, %(district_name)s, %(address)s,
                    %(community)s, %(total_price_10K)s, %(transaction_date)s,
                    %(unit_price)s, %(total_tsubo)s, %(real_space_percent)s,
                    %(type)s, %(house_age)s, %(level)s, %(main_purpose)s,
                    %(transaction_target)s, %(building_structure)s,
                    %(car_parking_price_10K)s, %(manage_unit_yn)s,
                    %(elevator_yn)s, %(note)s, %(scraped_time)s
                )
            """
            # 3. 逐筆匯入
            for cid, rec in data_dict.items():
                row = rec.copy()
                row["city_district_id"] = cid
                try:
                    cursor.execute(insert_sql, row)
                    success_cnt += 1
                except Exception as e:
                    ##收集失敗列的關鍵識別資訊
                    fail_logs.append(f"Insert Error at id:{cid}, batch:{row['batch_id']}, page:{row['page']}, {row['city_name']}-{row['district_name']}, Error Message: {e}>>>\n")
                    ##不rollback，直接跳下一筆
            conn.commit()##一次性提交所有成功的列
        # 組合回傳訊息
        if fail_logs:
            condition_insert = (f"MySQL Insert 完成：成功 {success_cnt} 筆，失敗 {len(fail_logs)} 筆###\n".join(fail_logs) + "###\n")
        else:
            condition_insert = (f"MySQL Insert Success: {city_name}-{district_name}:"f"{success_cnt} rows inserted.###\n")
    except Exception as insert_ex:
        # 重大錯誤才全體 rollback
        conn.rollback()##HERE!!!
        condition_insert = f"MySQL Insert Error (fatal): {insert_ex}>>>\n"
    return condition_insert

async def mysql_update_check_list(conn, target_table: str, table_name: str, city_name: str, district_name: str) -> str:
    """Update Check List for ETL Re-Run Check"""
    cond_mysql_update_check_list =""
    try:
        with conn.cursor() as cursor:
            count_sql = f"SELECT COUNT(*) AS cnt FROM {target_table} WHERE CITY_NAME = '{city_name}' AND DISTRICT_NAME = '{district_name}';"
            
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(count_sql)
            result = cursor.fetchone()
            count = result["cnt"]
            row_count = count
            done_yn = "Y"
            update_sql = f"""UPDATE {table_name} 
                             SET SCRAPED_TIME = '{current_time}',
                                 TOTAL_ROW_COUNT = {row_count},
                                 DONE_YN = '{done_yn}'  
                             WHERE CITY_NAME = '{city_name}' AND DISTRICT_NAME = '{district_name}';"""
            cursor.execute(update_sql)
            conn.commit()
        cond_mysql_update_check_list = f"{city_name}-{district_name}-Checked, count: {row_count}###\n"

    except Exception as ex_mysql_update_check_list:
        cond_mysql_update_check_list = f"MySQL Update Error (fatal): {ex_mysql_update_check_list}>>>\n"
    return cond_mysql_update_check_list

async def get_incomplete_city_district(conn, table_name: str) -> dict[int, dict]:
    """取得尚未完成爬取的縣市鄉鎮區"""
    cond_get_incomplete_city_district = ""
    incomplete_data = {}
    try:
        with conn.cursor() as cursor:
            query = f"""
                SELECT RECORD_ID, CITY_NAME, DISTRICT_NAME
                FROM {table_name}
                WHERE SCRAPED_TIME IS NULL AND TOTAL_ROW_COUNT IS NULL AND DONE_YN IS NULL
                ORDER BY RECORD_ID
            """
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                incomplete_data[row["RECORD_ID"]] = {
                    "city": row["CITY_NAME"],
                    "district": row["DISTRICT_NAME"]
                }
            min_query = f"""SELECT RECORD_ID, CITY_NAME, DISTRICT_NAME FROM {table_name}
                            WHERE RECORD_ID = (SELECT MIN(RECORD_ID) AS RECORD_ID FROM {table_name} WHERE SCRAPED_TIME IS NULL AND TOTAL_ROW_COUNT IS NULL AND DONE_YN IS NULL);"""
            cursor.execute(min_query)
            min_result = cursor.fetchone()
            if min_result:
                min_record_id = min_result["RECORD_ID"]
                min_city_name = min_result["CITY_NAME"]
                min_district_name = min_result["DISTRICT_NAME"]
            max_query = f"""SELECT RECORD_ID, CITY_NAME, DISTRICT_NAME FROM {table_name}
                            WHERE RECORD_ID = (SELECT MAX(RECORD_ID) AS RECORD_ID FROM {table_name} WHERE SCRAPED_TIME IS NULL AND TOTAL_ROW_COUNT IS NULL AND DONE_YN IS NULL);"""
            cursor.execute(max_query)
            max_result = cursor.fetchone()
            if max_result:
                max_record_id = max_result["RECORD_ID"]
                max_city_name = max_result["CITY_NAME"]
                max_district_name = max_result["DISTRICT_NAME"]
        cond_get_incomplete_city_district = f"In Current Loop Start From: {min_record_id}-{min_city_name}-{min_district_name} ~ {max_record_id}-{max_city_name}-{max_district_name}"
    except Exception as e:
        print(f"[ERROR] get_incomplete_city_district: {e}")
    return cond_get_incomplete_city_district, incomplete_data
##======>>>12. Insert Into MySQL
##======>>>Main()
async def main():
    try:
        async with async_playwright() as async_p:
            ##Record Begin Log
            await log_record(program_begin_log)

            ##Declare async_program_log to record async loop event log
            condition_1, get_cd_browser, get_cd_driver = await async_open_website(async_p, "https://lvr.land.moi.gov.tw/")          
            await log_record(condition_1)

            condition_2, city_district_data = await get_city_district_list(get_cd_browser, get_cd_driver)
            await log_record(condition_2)
            
            condition_2_1, count = await my_sql_check_list_count(mysql_conn, "INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST")
            await log_record(condition_2_1)

            if count == 0: ##代表第一次跑，才要重新匯入check list
                condition_2_2 = await mysql_insert_check_list(mysql_conn, city_district_data, "INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST")
                await log_record(condition_2_2)
            elif count == 365:
                condition_2_2 = f"Not First Time Run, Continue from Last City/District###\n"
                await log_record(condition_2_2)
            ##======>>>這邊到時候要做防呆，如果FULL RUN在INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST DONE_YN沒有'Y'，從沒有的開始跑
            ##2025-07-11 19:40 HOTFIX
            ##testing_datasets = {target:city_district_data[target] for target in range(1, 7 + 1)}##Only Partial Datasets
            cond_get_incomplete_city_district, city_district_data = await get_incomplete_city_district(mysql_conn, "INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST")
            print(cond_get_incomplete_city_district)
            await log_record(cond_get_incomplete_city_district)
            ##======>>>這邊到時候要做防呆，如果FULL RUN在INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST DONE_YN沒有'Y'，從沒有的開始跑
            for data in city_district_data:
                ##For Console Show Info:
                print(f"{city_district_data[data]['city']}-{city_district_data[data]['district']} begins:{datetime.now()}")
                
                async_batch_program_log = ""
                condition_3, get_cd_lastpage_browser, get_cd_lastpage_driver = await async_open_website(async_p, "https://lvr.land.moi.gov.tw/")
                async_batch_program_log += condition_3

                condition_4 = await prepare_search_bar(get_cd_lastpage_browser, get_cd_lastpage_driver, city_district_data[data]['city'], city_district_data[data]['district'], customCheck1, customCheck2, customCheck3, customCheck4, previous_years)
                async_batch_program_log += condition_4

                ##2025-07-11 HOTFIX======>>>
                condition_5, get_lastpage_num = await get_last_page_num(get_cd_lastpage_browser, get_cd_lastpage_driver)
                async_batch_program_log += condition_5
                if get_lastpage_num is None:
                    await get_cd_lastpage_browser.close()
                    city_district_no_data = f">>>\n{city_district_data[data]['city']}-{city_district_data[data]['district']} begins:{datetime.now()}, HAS NO DATA, please varified!!!\n>>>"
                    print(city_district_no_data)
                    await log_record(city_district_no_data)
                    update_log = await mysql_update_check_list(mysql_conn, "INTERIOR_WEB_RE_TRANSACTION_FULL_STAGE", "INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST", city_district_data[data]['city'], city_district_data[data]['district'])
                    async_batch_program_log += update_log
                    await log_record(async_batch_program_log)
                    continue
                else:
                ##2025-07-11 HOTFIX======>>>
                    condition_6, batch_dict = await establish_batch(get_lastpage_num)
                    async_batch_program_log += condition_6

                    tasks = [asyncio.create_task(exec_batch(async_p, batch_info, city_district_data[data]['city'], city_district_data[data]['district'])) 
                        for batch_info in batch_dict.values()]
                    
                    results = await asyncio.gather(*tasks)
                    ##===>>>資料集中處理，串接logs & 重新編號
                    full_batch_data = {}
                    idx = 1
                    for batch_log, batch_records in results:
                        async_batch_program_log += batch_log##併入總log
                        ##重新編號，防止key衝突
                        for _, rec in batch_records.items():
                            full_batch_data[idx] = rec
                            idx += 1
                    ##===>>>資料集中處理，串接logs & 重新編號                                 ####HOTFIX: 2025-07-13 07:33 CHANGE TB NAME
                    insert_log = await mysql_insert_transaction(mysql_conn, full_batch_data,"INTERIOR_WEB_RE_TRANSACTION_FULL_STAGE", city_district_data[data]['city'], city_district_data[data]['district'])
                    async_batch_program_log += insert_log
                    update_log = await mysql_update_check_list(mysql_conn, "INTERIOR_WEB_RE_TRANSACTION_FULL_STAGE", "INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST", city_district_data[data]['city'], city_district_data[data]['district'])
                    async_batch_program_log += update_log
                    await log_record(async_batch_program_log)
                    print(f"{city_district_data[data]['city']}-{city_district_data[data]['district']} Ends:{datetime.now()}")
           
            program_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
            ##program_end_log += f"End at: {program_end_time}"
            program_end_log = f"End at: {program_end_time}"##2025-07-05未宣告之前不能夠+=
            await log_record(program_end_log)
            if mysql_conn: ##確保連線存在
                mysql_conn.close() ##予以關閉
    except Exception as main_ex:
        err_detail = tb.format_exc()
        error_in_main = f"Error During Func: main(), Type:{type(main_ex).__name__}, Error Message: {main_ex}, Trace Back: {err_detail}>>>"
        await log_record(error_in_main)
        sys.exit(1)##Here特別標註
if __name__ == "__main__":
    ##pass
    asyncio.run(main())
##======>>>Main()