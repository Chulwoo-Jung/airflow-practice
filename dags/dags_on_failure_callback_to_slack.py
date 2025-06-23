from datetime import timedelta
import pendulum
from practical.on_failure_callback_to_slack import on_failure_callback_to_slack
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id='dags_on_failure_callback_to_slack',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    schedule='0 * * * *',
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_slack,
        'execution_timeout': timedelta(seconds=60)
    }

) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90',    # sleep longer than execution_timeout 60s
    )

    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_ext_1',
        bash_command='exit 1'   # exit 1 means failure
    )

    task_slp_90 >> task_ext_1