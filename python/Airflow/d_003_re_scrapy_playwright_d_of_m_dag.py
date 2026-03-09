from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from datetime import timedelta
import subprocess
import pendulum

TZ = pendulum.timezone("Asia/Taipei")

# 注意：不要在 default_args 放 start_date，避免和 DAG 等級衝突
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

def guard_only_2_12_22():
    now = pendulum.now(TZ)
    if now.day not in {2, 12, 22}:
        # 就算被誤觸發也直接跳過，不做任何實際動作
        raise AirflowSkipException(f"Guard: {now.to_datetime_string()} 非 2/12/22，跳過。")

def run_re_scrapy_dom():
    """執行本月資料爬取"""
    result = subprocess.run(
        ["python3", "Py_RE_Scrapy_Playwright_T_5Y_Zero_Retry_D_M.py"],
        cwd="/opt/airflow/dags",
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise Exception("Monthly RE Scrapy Script Failed")

with DAG(
    dag_id="d_003_re_scrapy_playwright_d_of_m_dag",
    ##start_date=pendulum.datetime(2025, 8, 21, 0, 0, tz=TZ),
    start_date=pendulum.datetime(2025, 8, 12, 0, 0, tz=TZ),  # 台北時間的午夜
    schedule='0 0 2,12,22 * *',   # 每月 2、12、22 號 00:00
    catchup=False,
    max_active_runs=1,
    ##dagrun_timeout=timedelta(hours=6),##可依需要放寬/縮短
    default_args=default_args,
    tags=['day_of_monthly', 'real_estate'],
) as dag:

    t_guard = PythonOperator(
        task_id='guard_only_2_12_22',
        python_callable=guard_only_2_12_22
    )

    t_run = PythonOperator(
        task_id='run_re_scrapy_dom',
        python_callable=run_re_scrapy_dom
    )

    t_trigger_exec_uSP = TriggerDagRunOperator(
        task_id='trigger_exec_uSP',
        trigger_dag_id='d_004_py_re_exec_uSP_dag',
        wait_for_completion=False
    )

    t_guard >> t_run >> t_trigger_exec_uSP
