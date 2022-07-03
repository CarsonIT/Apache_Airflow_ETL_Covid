######  Đây là file DAG ETL dữ liệu covid của các nước trên thế giới từ ngày 01/01/2020 đến ngày 16/06/2022 
import time
import requests 
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import json
import csv, urllib.request
import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from urllib.request import urlopen, URLError
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# Hàm gửi tin nhắn qua ứng dụng Slack khi Task có tình trạng failed
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = """
Task Failed. 
*DAG*: {dag_id} 
*Task*: {task}  
*Dag*: {dag} 
*Execution Time*: {exec_date}  
*Log Url*: {log_url} 
    """.format(
        dag_id=context.get('dag').dag_id,
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow_monitor')
    return failed_alert.execute(context=context)

# Hàm kết nối với cơ sở dữ liệu DestinationCovid
def connection_des():
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')
    return engine_des

# Hàm kết nối với cơ sở dữ liệu StageCovid
def connection_stage():
    conn_stage = BaseHook.get_connection('StageCovid')
    engine_stage = create_engine(f'postgresql://{conn_stage.login}:{conn_stage.password}@{conn_stage.host}:{conn_stage.port}/{conn_stage.schema}')
    return engine_stage

# Hàm  rút trích từ nguồn về DB StageCovid
def Load_to_Stage(**context):
    try:
        # Lấy ra ngày chạy DAG
        date_run = context["execution_date"].strftime("%Y-%m-%d")
        # Đọc dữ liệu từ nguồn và lấy ra đúng những dòng dữ liệu của ngày chạy DAG
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
        df_tg = pd.read_csv(url)
        df_tg = df_tg.loc[df_tg['date'] == date_run]
        # Kết nối vào DB Stage và đổ dữ liệu vào Stage
        engine = connection_stage()
        df_tg.to_sql('World_Covid', engine, if_exists='replace', index=False)
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

# Hàm chuyển đổi dữ liệu covid thế giới
def Transform_Case_Vac_TheGioi(**context):
    try:
        # Tạo các kết nối đến DB StageCovid và DestinationCovid
        engine_des = connection_des()
        engine_stage = connection_stage()

        # Đọc các dữ liệu từ Stage để bắt đầu quá trình transform
        df_case_vac = pd.read_sql("select * from \"World_Covid\"", engine_stage); 
        df_case_vac = df_case_vac[['location','date','total_cases','new_cases','total_deaths','new_deaths','total_vaccinations','people_vaccinated','people_fully_vaccinated']]
        df_country = pd.read_sql("select * from \"Country\"",engine_des)
        # Nếu dữ liệu bị null thì đổi thành 0
        df_case_vac['total_cases'] = df_case_vac['total_cases'].fillna(0)
        df_case_vac['new_cases'] = df_case_vac['new_cases'].fillna(0)
        df_case_vac['total_deaths'] = df_case_vac['total_deaths'].fillna(0)
        df_case_vac['new_deaths'] = df_case_vac['new_deaths'].fillna(0)
        df_case_vac['total_vaccinations'] = df_case_vac['total_vaccinations'].fillna(0)
        df_case_vac['people_vaccinated'] = df_case_vac['people_vaccinated'].fillna(0)
        df_case_vac['people_fully_vaccinated'] = df_case_vac['people_fully_vaccinated'].fillna(0)
        #  Đổi tên cột và chuyển kiểu dữ liệu
        df_case_vac = df_case_vac[df_case_vac["location"].str.contains("High income|International|Low income|Lower middle income|Upper middle income|World|Asia|Africa|Europe|North America|South America") == False]
        df_case_vac['date'] = pd.to_datetime(df_case_vac['date']).dt.date 

        # lấy mã nước để tham chiếu
        join_temp_1 = pd.merge(df_case_vac, df_country, how='inner', left_on = ('location'), right_on = ('CountryName'), suffixes=('_left','_right'))
        df_case_vac =  join_temp_1[['CountryID','date','total_cases','new_cases','total_deaths','new_deaths','total_vaccinations','people_vaccinated','people_fully_vaccinated']] 
        # lấy ngày chạy DAG và tạo một file csv để lưu dữ liệu sau khi transform
        date_run = context['ds']
        file = 'tg_covid_files/tg_' + date_run + '.csv'
        df_case_vac.to_csv(file,index=False, encoding="utf-8")
        # push đường dẫn file lên xcom
        context['ti'].xcom_push(key="file_path_covid_TG", value = file)

    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

# Hàm nạp dữ liệu về DB DestinationCovid 
def Load_Case_Vac(**context):
    try:
        # Kết nối DB DestinationCovid
        engine_des = connection_des()
        # Lấy đường dẫn file lưu dữ liệu sau khi transform và đổ toàn bộ dữ liệu ở file vào DestinationCovid
        value_path = context['ti'].xcom_pull(
        task_ids = 'Transform_Case_Vac_TheGioi',
        key = "file_path_covid_TG"
        )
        df = pd.read_csv(value_path)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.to_sql('World_Covid_Des', engine_des, if_exists='append', index=False)
      
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False


# Tạo DAG Historical_World_ETL_Data, với số lần chạy DAG nhiều nhất trong 1 thời điểm là 1 lần (max_active_runs=1), catchup=True để thực hiện backfilling để chạy DAG trong quá khứ từ ngày 01/01/2020 đến ngày 16/06/2022 và mỗi ngày 1 lần chạy DAG (schedule_interval = "@daily"), thêm tags để dễ dàng tìm kiếm DAG trên giao diện web Airflow 
with DAG(dag_id="Historical_World_ETL_Data", max_active_runs=1 ,schedule_interval = "@daily", start_date=datetime(2020, 1, 1), end_date=datetime(2022, 6, 16),catchup=True,  tags=["Airflow_ETL_"]) as dag:
    # task rút trích dữ liệu từ nguồn đổ vào DB StageCovid
    Load_to_Stage = PythonOperator(
        task_id ='Load_to_Stage',
        python_callable=Load_to_Stage,
        on_failure_callback = task_fail_slack_alert,
        execution_timeout=timedelta(seconds=5*60),
        dag = dag,
    ) 
    # task chuyển đổi dữ liệu đã rút trích 
    Transform_Case_Vac_TheGioi = PythonOperator(
        task_id ='Transform_Case_Vac_TheGioi',
        python_callable=Transform_Case_Vac_TheGioi,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    )
    # task nạp dữ liệu vào DB DestinationCovid
    Load_Case_Vac = PythonOperator(
        task_id ='Load_Case_Vac',
        python_callable=Load_Case_Vac,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    )
    # Cài đặt luồng thực hiện task
    Load_to_Stage >> Transform_Case_Vac_TheGioi >> Load_Case_Vac