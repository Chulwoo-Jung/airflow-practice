from operator.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
from airflow.sdk import DAG

with DAG(
    dag_id='dags_seoul_property_transaction',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    seoul_property_transaction = SeoulApiToCsvOperator(
        task_id='seoul_property_transaction',
        dataset_nm='tbLnOpendataRtmsV',
        path='/opt/airflow/files/seoulPropertyTransaction/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}',
        file_name='seoulPropertyTransaction.csv'
    )

    seoul_property_transaction
    
  