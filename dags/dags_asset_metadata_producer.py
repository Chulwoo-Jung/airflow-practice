from airflow.sdk import DAG, Asset, task, Metadata
import pendulum
from zlib import crc32
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator

tvCorona19VaccinestatNew = Asset("tvCorona19VaccinestatNew")

with DAG(
        dag_id='dags_asset_metadata_producer',
        schedule=None,
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        catchup=False,
        tags=['asset','producer']
) as dag:
    
    seoulApiToCsvOperator = SeoulApiToCsvOperator(
        task_id='seoulApiToCsvOperator',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    @task(task_id='task_producer_metadata', outlets=[tvCorona19VaccinestatNew])
    def task_producer_metadata(**kwargs):
        dt = kwargs.get('data_interval_end').in_timezone('Europe/Berlin').strftime('%Y%m%d')
        file = f'/opt/airflow/files/tvCorona19VaccinestatNew/{dt}/tvCorona19VaccinestatNew.csv'
        with open(file) as f:
            contents = f.read()
            crc = crc32(contents.encode())
            cnt = len(contents.split('\n')) - 1
            print('file_name: Corona19VaccinestatNew')
            print('crc32: {}'.format(crc))
            print('row_count: {}'.format(cnt))
        
        kwargs['outlet_events'][tvCorona19VaccinestatNew].extra = {"len_of_file": cnt, 'crc32': crc, 'file_path': file}

    seoulApiToCsvOperator >> task_producer_metadata()