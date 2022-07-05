
import datetime

from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="Example_Demo1",
    start_date=datetime.datetime(2022, 7, 2),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)

def wait_for_data_new():
    if (1==1):
        return False
    else:
        return False

def abc():
    print("abc")
        


# [START howto_branch_datetime_operator]
wait_for_data_new = PythonSensor(
    task_id="wait_for_data_new",
    python_callable=wait_for_data_new,
    dag=dag,
    )

extract = PythonOperator(
    task_id="extract",
    python_callable=abc,
    dag=dag,
    )




# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run dummy_task_1 if cond2 executes between 15:00:00, and 00:00:00 of the following day
wait_for_data_new >> extract