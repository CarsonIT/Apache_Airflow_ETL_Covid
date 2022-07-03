
import datetime

from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="Example_Demo",
    start_date=datetime.datetime(2022, 7, 2),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)

# [START howto_branch_datetime_operator]
dummy_task_1 = DummyOperator(task_id='task1_example', dag=dag)
dummy_task_2 = DummyOperator(task_id='task2_example', dag=dag)



# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run dummy_task_1 if cond2 executes between 15:00:00, and 00:00:00 of the following day
dummy_task_1 >> dummy_task_2