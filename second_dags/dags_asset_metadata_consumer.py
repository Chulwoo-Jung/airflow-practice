from airflow.sdk import Asset, DAG, task
import pendulum
from zlib import crc32


seoul_api_rt_bicycle_info = Asset('asset_seoul_api_rt_bicycle_info')

with DAG(
        dag_id='dags_asset_metadata_consumer',
        schedule=[seoul_api_rt_bicycle_info],
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:3.0','asset','consumer','metadata']
) as dag:
   
    @task(task_id='task_consumer_with_metadata',
          inlets=[seoul_api_rt_bicycle_info])
    def task_consumer_with_metadata(**kwargs):
        inlet_events = kwargs.get('inlet_events')
        events = inlet_events[seoul_api_rt_bicycle_info]
        print('::group::Dataset Event List')
        for i in events:
            print(i)
        print('::endgroup::')
        crc_val_from_ds = events[-1].extra['crc32']
        file_path = events[-1].extra['file_path']
        with open(file_path) as f:
            contents = f.read()
            crc_val_from_file = crc32(contents.encode())

        print('::group::CRC verification process start')
        print(f'CRC of ds: {crc_val_from_ds}')
        print(f'CRC of file: {crc_val_from_file}')
        if crc_val_from_file == crc_val_from_ds:
            print('CRC verification Success')
        else:
            print('CRC verification Faile.')
        print('::endgroup::')

    task_consumer_with_metadata()