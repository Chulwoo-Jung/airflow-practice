from datetime import timedelta
import pendulum
from practical.on_failure_callback_to_kakao import on_failure_callback_to_kakao

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='dags_on_failure_callback_to_kakao',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    schedule='*/20 * * * *',
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=60)
    }

) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90',
    )

    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_ext_1',
        bash_command='exit 1'
    )

    task_slp_90 >> task_ext_1