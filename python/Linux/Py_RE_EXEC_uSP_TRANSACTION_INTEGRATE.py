##======>>>1. IMPORT
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
import csv
import pymysql
import traceback as tb
##======>>>1. IMPORT
LOG_FILE_NAME = f"Py_RE_EXEC_uSP_TRANSACTION_INTEGRATE_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}_LOG.txt"##2025-08-12，以_LOG.TXT做結尾，好讓之後的HOUSE KEEPING可以做LOG檔整理
LOG_FILE_PATH = Path(LOG_FILE_NAME) 
##======>>>2. LOG FUNCTION
def log_record(msg: str):
    """Write Each State's Program Logs, Return Nothing"""
    try:
        with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
            f.write(msg)
    except Exception as e:
        print(f"log_record 寫入失敗: {e}")
##======>>>2. LOG FUNCTION
##======>>>3. DATABASE CONNECTION & SEARCH BAR PARAMS
##Load In MySQL Connection Params
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

def py_mysql_connect(HOST, PORT, USER, PASSWORD, DB_NAME) -> Tuple[str, Optional[pymysql.connections.Connection]]:
    """Establish MySQL Connection, Return Conn"""
    condition_py_mysql_connect = ''
    try:
        ##由於平均每一縣市、鄉鎮區會需要約1分鐘，故365 = 6小時多，設定為7小時，以防萬一，給予充足的時間
        conn = pymysql.connect(host=HOST,port=PORT,user=USER,password=PASSWORD, database=DB_NAME,cursorclass=pymysql.cursors.DictCursor, read_timeout=25200, write_timeout=25200, connect_timeout=60)##7hrs
        condition_py_mysql_connect = f"py_mysql_connect Successful Connected, DB Name: {conn.db.decode()}###\n"
    except Exception as py_mysql_connect_ex:
        condition_py_mysql_connect = f"py_mysql_connect Error: {py_mysql_connect_ex}>>>\n"
        conn = None
    return condition_py_mysql_connect, conn
##======>>>3. DATABASE CONNECTION

def main():
    try:
        ##Establish Program Begin Time
        program_beg_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        program_begin_log = f"Program: Py_RE_EXEC_uSP_TRANSACTION_INTEGRATE.py, begins at: {program_beg_time}###\n"
        log_record(program_begin_log)

        cond_load_pymysql_params, mysql_params = load_params("py_mysql_tw_real_estate_params.csv")
        log_record(cond_load_pymysql_params)
        ##Get Params for DB Connection
        host = mysql_params.get('host')
        port = int(mysql_params.get('port'))
        user = mysql_params.get('user')
        password = mysql_params.get('password')
        db_name = mysql_params.get('db_name')

        cond_py_mysql_connect, mysql_conn = py_mysql_connect(host, port, user, password, db_name)
        log_record(cond_py_mysql_connect)
        

        begin_proc = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        log_record(f"Begin EXEC uSP_TRANSACTION_INTEGRATE: {begin_proc}###\n")
        with mysql_conn.cursor() as cursor:
            ##Step 1:設定OUT變數名稱
            cursor.execute("SET @out_result_msg = '';")   
            ##Step 2:呼叫Procedure，指定OUT變數
            cursor.execute("CALL uSP_TRANSACTION_INTEGRATE(@out_result_msg);")
            ##Step 3:把OUT變數抓出來
            cursor.execute("SELECT @out_result_msg;")
            result = cursor.fetchone()
            if result:
                result_msg = result['@out_result_msg']  # 拿到回傳的文字字串
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
                log_record(f"PROC uSP_TRANSACTION_INTEGRATE EXEC RESULT: {result_msg}, at: {now}###\n")
                print(f"PROC uSP_TRANSACTION_INTEGRATE EXEC RESULT: {result_msg}, at: {now}###\n")
        mysql_conn.commit()
        
    except Exception as main_ex:
        err_detail = tb.format_exc()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-2]
        error_in_main = f"Error During Func: main(), Type:{type(main_ex).__name__}, Error Message: {main_ex}, Trace Back: {err_detail}, at: {now}>>>\n"
        log_record(error_in_main)
    finally:
        mysql_conn.close()

if __name__ == '__main__':
    main()