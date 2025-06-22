import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id='tvCorona19VaccinestatNew_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, # 1 Day
        mode='reschedule'
    )