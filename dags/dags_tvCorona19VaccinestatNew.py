from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
from airflow.sdk import DAG

with DAG(
    dag_id='dags_tvCorona19VaccinestatNew',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    tvCorona19VaccinestatNew = SeoulApiToCsvOperator(
        task_id='tvCorona19VaccinestatNew',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tvCorona19VaccinestatNew
    
  