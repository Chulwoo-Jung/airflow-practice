import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dags_time_sensor_with_async",
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    end_date=pendulum.datetime(2025, 6, 1, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )