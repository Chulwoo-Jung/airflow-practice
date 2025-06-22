import pendulum
from datetime import timedelta
from airflow.utils.state import State
from airflow.sdk import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id='dags_external_task_sensor',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_a',
        allowed_states=[State.SKIPPED], # If the task_a is skipped, then this task will be successful
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10 seconds
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_b',
        failed_states=[State.SKIPPED], # If the task_b is skipped, then this task will be failed
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10 seconds
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_c',
        allowed_states=[State.SUCCESS], # If the task_c is successful, then this task will be successful
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10 seconds
    )