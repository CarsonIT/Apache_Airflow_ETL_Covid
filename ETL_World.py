######  Đây là file DAG ETL dữ liệu covid của các nước trên thế giới từ ngày 17/06/2022 trở đi
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
import numpy as np
from psycopg2.extensions import register_adapter, AsIs


#[Hàm gửi tin nhắn qua ứng dụng Slack khi Task có tình trạng failed]
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


# Hàm kiểm tra trên nguồn covid thế giới có dữ liệu mới hay chưa 
def wait_for_data_source_new():
    try:
        # Kết nối và đọc dữ liệu từ nguồn covid thế giới
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
        df_source = pd.read_csv(url)
        df_source.rename(columns = {'last_updated_date':'date'}, inplace = True)
        # Kết nối đến DB StageCovid
        engine_stage = connection_stage()
        # Lấy dữ liệu của bảng World_Covid trong stage
        df_stage = pd.read_sql("select * from \"World_Covid\"", engine_stage); 
        # Ép kiểu của stage cho giống source để so sánh 
        df_stage = df_stage.astype(df_source.dtypes)

        # Tạo 1 df để lấy những dòng khác nhau giữa Stage và Source
        df_diff = pd.concat([df_source,df_stage]).drop_duplicates(keep=False)

        # Kiểm tra dữ liệu ở source có giống stage không
        if df_diff.empty:
            return True # source giống stage => Không có cập nhật --> return về false để chờ tiếp
        else:
            return True      # source khác stage => có cập nhật dữ liệu--> return về true để chạy tiếp luồng ETL
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False


def Extract_Case_Vac_TheGioi():
    try:
        # Kết nối và đọc dữ liệu từ nguồn covid thế giới
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
        df_source = pd.read_csv(url)
        df_source.rename(columns = {'last_updated_date':'date'}, inplace = True)
        # Kết nối đến DB StageCovid
        engine_stage = connection_stage()
        # Lấy dữ liệu của bảng World_Covid trong stage
        # truncate table World_Covid và đổ dữ liệu mới vào --> chiến thuật Incremental Extract
        df_source.to_sql('World_Covid', engine_stage, if_exists='replace', index=False)
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

    
# Hàm nạp dữ liệu về DB DestinationCovid trong trường hợp đã có dữ liệu của ngày này trong DestinationCovid
def Load_Case_Vac_TheGioi(**context):
    # Kết nối DB DestinationCovid
    engine_des = connection_des()
   # Lấy đường dẫn file lưu dữ liệu sau khi transform và đọc dữ liệu từ file này
    value_path = context['ti'].xcom_pull(
        task_ids = 'Transform_Case_Vac_World',
        key = "file_path_covid_TG"
    )
    df = pd.read_csv(value_path)
    df['CountryID']= df['CountryID'].astype(int)
    df['total_cases']= df['total_cases'].astype(int)
    df['new_cases']= df['new_cases'].astype(int)
    df['total_deaths']= df['total_deaths'].astype(int)
    df['new_deaths']= df['new_deaths'].astype(int)
    df['total_vaccinations']= df['total_vaccinations'].astype(int)
    df['people_vaccinated']= df['people_vaccinated'].astype(int)
    df['people_fully_vaccinated']= df['people_fully_vaccinated'].astype(int)

    dbconnect = engine_des.raw_connection()

    # Đọc từng dòng trong file, nếu có trong des thì update dòng đó, không thì insert
    for i in range(len(df)):
        
        row = df.loc[i]
        row = pd.DataFrame(row)
        row = row.transpose()
        print(" Thong tin kieu du lieu", row.info())
        print (" từng dòng trong row", row)
        print("Country ID ne Phuong: ", str(row['CountryID'][i]))
        print("Son Pause")
        query = "select * from \"World_Covid_Des\"  where \"CountryID\" =" + str(row['CountryID'][i]) + "and date = '"+ str(row['date'][i]) +"'"
        row_des = pd.read_sql(query, engine_des)
        if len(row_des) == i: # Nếu quốc gia đó vào ngày đó chưa có trong Des
            row.to_sql('World_Covid_Des', engine_des, if_exists='append', index=False) # insert dòng đó vào Des
        else: 
            query= "update \"World_Covid_Des\" set total_cases=" + str(row['total_cases'][i]) + ",new_cases =" + str(row['new_cases'][i]) + ", total_deaths ="+ str(row['total_deaths'][i]) + ", new_deaths ="+ str(row['new_deaths'][i]) + ", total_vaccinations ="+ str(row['total_vaccinations'][i]) +  ", people_vaccinated ="+ str(row['people_vaccinated'][i]) +  ", people_fully_vaccinated ="+ str(row['people_fully_vaccinated'][i]) + " where \"CountryID\" = " + str(row['CountryID'][i]) + "and date = '"+ row['date'][i] +"'"
            cursor = dbconnect.cursor()
            cursor.execute(query)



# Tạo DAG ETL_World, được lập lịch chạy vào mỗi ngày vào các thời điểm: 0:00, 3:00, 10:00, 12:00, 12:00, 14:00, 17:00, 22:00 (UTC +0), ngày bắt đầu là 17/06/2022, và không thực hiện backfilling để chạy DAG trong quá khứ, thêm tags để dễ dàng tìm kiếm DAG trên giao diện web Airflow 
with DAG(dag_id="ETL_World",schedule_interval = "0 0,3,10,12,14,17,22 * * *", start_date=datetime(2022, 6, 17),catchup=False,  tags=["Airflow_ETL_"]) as dag:
    # task PythonSensor chờ nguồn có dữ liệu mới
    wait_for_data_case_vac_new = PythonSensor(
        task_id="wait_for_data_case_vac_new",
        python_callable=wait_for_data_source_new,
        timeout = 60*60,  # Timeout of 1 hours
        mode='reschedule', # Timeout of 1 hours
        on_failure_callback = task_fail_slack_alert,
        dag=dag,
    )
    # task rút trích dữ liệu từ nguồn đổ vào DB StageCovid
    Extract_Case_Vac_TheGioi = PythonOperator(
        task_id ='Extract_Case_Vac_TheGioi',
        python_callable=Extract_Case_Vac_TheGioi,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    ) 
    # task chuyển đổi dữ liệu đã rút trích 
    Transform_Case_Vac_TheGioi = PythonOperator(
        task_id ='Transform_Case_Vac_World',
        python_callable=Transform_Case_Vac_TheGioi,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    )
    
    # task thông báo quá trình ETL đã xong
    Load_Case_Vac_TheGioi = PythonOperator(
        task_id='Load_Case_Vac_TheGioi',
        python_callable = Load_Case_Vac_TheGioi,
        dag = dag,
        on_failure_callback = task_fail_slack_alert,
    )

    # Cài đặt luồng thực hiện task
    wait_for_data_case_vac_new >> Extract_Case_Vac_TheGioi >> Transform_Case_Vac_TheGioi >> Load_Case_Vac_TheGioi