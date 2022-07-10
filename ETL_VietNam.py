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
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.bash_operator import BashOperator

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

# Hàm đổ dữ liệu rút trích từ nguồn về DB StageCovid
def Load_to_Stage():
    try:
        # rut trich va load vao Stage data ca nhiem VN
        url = "https://covid19.ncsc.gov.vn/api/v3/covid/provinces"
        df = pd.read_json(url)    
        engine = connection_stage()
        df.to_sql('vietnam_case', engine,  if_exists='replace', index=False)

        # rut trich va load vao Stage data vaccine VN
        url = "https://covid19.ncsc.gov.vn/api/v3/vaccine/provinces"
        df = pd.read_json(url) 
        engine = connection_stage()
        df.to_sql('vietnam_vaccine', engine, if_exists='replace', index=False)
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

# Hàm kiểm tra dữ liệu trên nguồn ca nhiễm covid đã có dữ liệu mới chưa
def wait_for_data_case_new():
    try:
        # đọc dữ liệu từ nguồn 
        url = "https://covid19.ncsc.gov.vn/api/v3/covid/provinces"
        df = pd.read_json(url)  
        # Lấy ra ngày rút trích dữ liệu gần nhất từ variable datetime_extract_case_VN
        datetime_extract_case_VN = Variable.get('datetime_extract_case_VN')
        # So sánh ngày rút trích gần nhất với ngày của dữ liệu trong nguồn, 
            # case 1. nếu bằng nhau(tức đã rút trích) thì trả về false
            # case 2. nếu khác nhau (tức chưa rút trích) thì trả về true và cập nhật lại variable bằng ngày rút trích hiện tại
        if (datetime_extract_case_VN != df['last_updated'][0]):
            datetime_extract_case_VN = df['last_updated'][0]
            Variable.update(key='datetime_extract_case_VN', value=datetime_extract_case_VN)
            return True
        return False
        
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

# Hàm kiểm tra dữ liệu trên nguồn vaccine covid đã có dữ liệu mới chưa
def wait_for_data_vac_old():
    try:
        # đọc dữ liệu từ nguồn 
        url = "https://covid19.ncsc.gov.vn/api/v3/vaccine/provinces"
        df = pd.read_json(url)  
        # Lấy ra ngày rút trích dữ liệu gần nhất từ variable datetime_extract_vac_VN
        datetime_extract_vac_VN = Variable.get('datetime_extract_vac_VN')
        # So sánh ngày rút trích gần nhất với ngày của dữ liệu trong nguồn, 
            # case 1. nếu bằng nhau(tức đã rút trích) thì trả về false
            # case 2. nếu khác nhau (tức chưa rút trích) thì trả về true và cập nhật lại variable bằng ngày rút trích hiện tại
        if (datetime_extract_vac_VN != df['last_updated'][0]):
            datetime_extract_vac_VN = df['last_updated'][0]
            Variable.update(key='datetime_extract_vac_VN', value=datetime_extract_vac_VN)
            return True
        return False

    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

# Hàm chuyển đổi dữ liệu covid của các tỉnh thành Việt Nam
def Transform_Case_Vac_VN(**kwargs):
    # Tạo các kết nối đến DB StageCovid và DestinationCovid
    engine_des = connection_des()
    engine_stage = connection_stage()
    # Đọc dữ liệu đã được rút trích từ StageCovid
    df_case_vn = pd.read_sql("select * from \"vietnam_case\"", engine_stage); 
    df_vaccine_vn = pd.read_sql("select * from \"vietnam_vaccine\"", engine_stage); 

    # Lấy ra mã tỉnh để tham chiếu đến
    df_province = pd.read_sql("select * from \"Province\"",engine_des)
    # Join dữ liệu từ hai bảng ở StageCovid để đổ vào 1 bảng duy nhất ở DestinationCovid
    join_temp = pd.merge(df_case_vn, df_vaccine_vn, how='inner', left_on = ('name','last_updated'), right_on = ('name','last_updated'), suffixes=('_left','_right'))
    case_vaccine_VN =  join_temp[['name','case_yesterday','new_case','new_death','new_recovered','new_active','total_case','total_death','death_average_7days','case_average_7days','percent_case_population','lastOnceInjected','lastTwiceInjected','popOverEighteen','totalInjected','totalOnceInjected','totalTwiceInjected','totalVaccineProvided','totalVaccineAllocatedReality','totalVaccineAllocated','currentTeamInjectInPractice','last_updated']] 
    df_case_vaccine_VN = pd.DataFrame(case_vaccine_VN)
    join_temp_1 = df_case_vaccine_VN.set_index('name').join(df_province.set_index('ProvinceName'))
    case_vaccine_vn =  join_temp_1[['ProvinceID','case_yesterday','new_case','new_death','new_recovered','new_active','total_case','total_death','death_average_7days','case_average_7days','percent_case_population','lastOnceInjected','lastTwiceInjected','popOverEighteen','totalInjected','totalOnceInjected','totalTwiceInjected','totalVaccineProvided','totalVaccineAllocatedReality','totalVaccineAllocated','currentTeamInjectInPractice','last_updated']] 
    ### Thực hiện các Transform 
    # Nếu dữ liệu null thì thay bằng 0 và đổi kiểu dữ liệu của một số thuộc tính
    case_vaccine_vn['ProvinceID'] = case_vaccine_vn['ProvinceID'].fillna(0)
    case_vaccine_vn['ProvinceID']  = case_vaccine_vn['ProvinceID'] .astype('int64')
    case_vaccine_vn['last_updated'] = pd.to_datetime(case_vaccine_vn['last_updated'],format="%d/%m/%Y %H:%M:%S")
    case_vaccine_vn['date'] = case_vaccine_vn['last_updated']
    case_vaccine_vn['date'] = case_vaccine_vn['date'].dt.strftime('%Y/%m/%d')
    # lấy ngày chạy DAG và tạo một file csv để lưu dữ liệu sau khi transform
    date_run = kwargs['ds']
    file = 'vn_covid_files/vn_' + date_run + '.csv'   
    case_vaccine_vn.to_csv(file,index=False, encoding="utf-8")
    # push đường dẫn file lên xcom
    kwargs['ti'].xcom_push(key="file_path_covid_VN", value = file)

# Hàm kiểm tra trong DB DestinationCovid đã có dữ liệu của ngày này chưa
def ComparingDataOld_New(**context):

    try:
        # Kết nối đến DB DestinationCovid
        engine_des = connection_des()
        # Pull đường dẫn file lưu dữ liệu sau khi transform từ xcom về để đọc dữ liệu từ file này
        value_path = context['ti'].xcom_pull(
        task_ids = 'Transform_Case_Vac_VN',
        key = "file_path_covid_VN"
        )
        df = pd.read_csv(value_path)
        df['date'] = pd.to_datetime(df['date']).dt.date
        # Lấy ra ngày của các dòng dữ liệu trong file
        date_file = df['date'][0]
        # Lấy ra ngày gần nhất của dữ liệu DB DestinationCovid
        df_des = pd.read_sql("select date from \"VietNam_Covid_Des\" order by date desc", engine_des); 
        df_des['date'] = pd.to_datetime(df_des['date']).dt.date
        date_des = df_des['date'][0]
        # So sánh ngày dữ liệu của file và DB DestinationCovid
        if date_file == date_des:
            return "data_old" # Trả về task data_old trong trường hợp đã có dữ liệu của ngày này trong DestinationCovid
        else:
            return "data_new" #  trả về task data_new trong trường hợp chưa có dữ liệu của ngày này trong DestinationCovid
        # truong hop chay dag lan dau, tuc chua co DBdes
    except Exception as e:
        return "data_new"  # Trường hợp bị lỗi do chưa có DB DestinationCovid vì đây là lần chạy DAG đầu tiên --> Cũng là trường hợp chưa có dữ liệu


# Hàm nạp dữ liệu về DB DestinationCovid trong trường hợp đã có dữ liệu của ngày này trong DestinationCovid
def _data_old(**context):
    # Kết nối DB DestinationCovid
    engine_des = connection_des()
   # Lấy đường dẫn file lưu dữ liệu sau khi transform và đọc dữ liệu từ file này
    value_path = context['ti'].xcom_pull(
        task_ids = 'Transform_Case_Vac_VN',
        key = "file_path_covid_VN"
    )
    df = pd.read_csv(value_path)
    # Xóa những dữ liệu cũ DB Destination và đổ dữ liệu mới vào
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['last_updated'] = pd.to_datetime(df['last_updated']) 
    date_file = df['date'][0]
    df_des = pd.read_sql("select * from \"VietNam_Covid_Des\" order by date desc", engine_des); 
    df_des['date'] = pd.to_datetime(df_des['date']).dt.date
    date_des = df_des['date'][0]
    dbconnect = engine_des.raw_connection()   
    query = "DELETE FROM \"VietNam_Covid_Des\" WHERE date = '" + str(date_des) + "'"
    cursor = dbconnect.cursor()
    cursor.execute(query)
    dbconnect.commit()
    df.to_sql('VietNam_Covid_Des', engine_des, if_exists='append', index=False)


# Hàm nạp dữ liệu về DB DestinationCovid trong trường hợp chưa có dữ liệu của ngày này trong DestinationCovid
def _data_new(**context):
    try:
        # Kết nối DB DestinationCovid
        engine_des = connection_des()
        # Lấy đường dẫn file lưu dữ liệu sau khi transform và đổ toàn bộ dữ liệu ở file vào DestinationCovid
        value_path = context['ti'].xcom_pull(
        task_ids = 'Transform_Case_Vac_VN',
        key = "file_path_covid_VN"
        )
        df = pd.read_csv(value_path)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        df.to_sql('VietNam_Covid_Des', engine_des, if_exists='append', index=False)

        
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False
    
# Hàm thông báo quá trình ETL hoàn thành
def Notify():
    print("Notication: Flow ETL data covid Done")


# Tạo DAG ETL_VietNam, được lập lịch chạy vào mỗi giờ một lần, ngày bắt đầu là 05/06/2022, và không thực hiện backfilling để chạy DAG trong quá khứ, thêm tags để dễ dàng tìm kiếm DAG trên giao diện web Airflow 
with DAG(dag_id="ETL_VietNam",schedule_interval="@hourly", start_date=datetime(2022, 6, 5),catchup=False,  tags=["Airflow_ETL_"]) as dag:
    # task PythonSensor chờ nguồn ca nhiễm covid có dữ liệu mới
    wait_for_data_case_new = PythonSensor(
        task_id="wait_for_data_case_new",
        python_callable=wait_for_data_case_new,
        timeout = 60*60,  # Timeout of 1 hours
        mode='reschedule', 
        on_failure_callback = task_fail_slack_alert,
        dag=dag,
    )
    # task PythonSensor chờ nguồn vaccine covid có dữ liệu mới
    wait_for_data_vac_old = PythonSensor(
        task_id="wait_for_data_vac_old",
        python_callable=wait_for_data_vac_old,
        timeout = 60*60,  # Timeout of 1 hours
        mode='reschedule', 
        on_failure_callback = task_fail_slack_alert,
        dag=dag,
    )
    # task rút trích dữ liệu từ nguồn đổ vào DB StageCovid
    Load_to_Stage = PythonOperator(
        task_id ='Load_to_Stage',
        python_callable=Load_to_Stage,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    ) 
    # task chuyển đổi dữ liệu đã rút trích 
    Transform_Case_Vac_VN = PythonOperator(
        task_id ='Transform_Case_Vac_VN',
        python_callable=Transform_Case_Vac_VN,
        on_failure_callback = task_fail_slack_alert,
        dag = dag,
    )
    # task nạp dữ liệu trong trường hợp đã có dữ liệu của ngày ngày trong DB DestinationCovid
    data_old = PythonOperator(
        task_id="data_old", 
        python_callable=_data_old,
        on_failure_callback = task_fail_slack_alert,
    )
    # task nạp dữ liệu trong trường hợp chưa có dữ liệu của ngày ngày trong DB DestinationCovid
    data_new = PythonOperator(
        task_id="data_new", 
        python_callable=_data_new,
        on_failure_callback = task_fail_slack_alert,
    )  
    # task kiểm tra dữ liệu đã rút trích đã có trên DB DestinationCovid chưa
    ComparingDataOld_New = BranchPythonOperator(
        task_id="ComparingDataOld_New", python_callable=ComparingDataOld_New
    )
    # task thông báo quá trình ETL đã xong
    notify = PythonOperator(
        task_id='notify_ETL_Done',
        python_callable = Notify,
        dag = dag,
        on_failure_callback = task_fail_slack_alert,
        trigger_rule="none_failed"
    )
    
    # Cài đặt luồng thực hiện task
    [wait_for_data_case_new, wait_for_data_vac_old] >> Load_to_Stage
    Load_to_Stage >> Transform_Case_Vac_VN >> ComparingDataOld_New >> [data_old, data_new] >> notify

    