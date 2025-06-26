import pendulum
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pandas as pd

from airflow.sdk import DAG, task, get_current_context


with DAG(
        dag_id="dags_dynamic_task_mapping_dict",
        schedule=None,
        start_date=pendulum.datetime(2025, 6, 1, tz="Europe/Berlin"),
        catchup=False,
        tags=['dnm_tsk_map','map_index']
) as dag:
    task_get_rt_bicycle_info = SeoulApiToCsvOperator(
        task_id='task_get_rt_bicycle_info',
        dataset_nm='bikeList',
        path='/opt/airflow/files/rt_bicycle_info/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeList.csv'
    )

    @task(task_id='task_read_csv_to_dict')
    def task_read_csv_to_dict(**kwargs):
        dt = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
        file = f'/opt/airflow/files/rt_bicycle_info/{dt}/bikeList.csv'
        bicycle_info_dict = pd.read_csv(file)[:100].to_dict(orient='index')
        return bicycle_info_dict
        
    @task(task_id='task_count_character',
          map_index_template="{{ station_name_index }}"
    )
    def task_station_info(station):
        values_dict = station[1]
        station_nm = values_dict.get('stationName')
        prk_cnt = values_dict.get('parkingBikeTotCnt')
        context = get_current_context()
        context["station_name_index"] = station_nm

        print(f'Bike Station: {station_nm} has {prk_cnt} bikes')

    task_read_csv_to_dict = task_read_csv_to_dict()
    task_get_rt_bicycle_info >> task_read_csv_to_dict >> task_station_info.expand(station=task_read_csv_to_dict)