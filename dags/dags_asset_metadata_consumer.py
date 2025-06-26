import pendulum
from zlib import crc32
from airflow.sdk import DAG, task, Asset

tvCorona19VaccinestatNew = Asset('tvCorona19VaccinestatNew')

with DAG(
        dag_id='dags_asset_metadata_consumer',
        schedule=[tvCorona19VaccinestatNew],
        catchup=False,
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        tags=['asset','consumer','metadata']
) as dag:
  
    @task(task_id='task_consumer_with_metadata', inlets=[tvCorona19VaccinestatNew])
    def task_consumer_with_metadata(**kwargs):
        inlet_events = kwargs.get('inlet_events')
        events = inlet_events[tvCorona19VaccinestatNew]
        print('::group::Asset Event List')
        for i in events[-5:]:   # print the latest 5 produced Asset
            print(i)
        print('::endgroup::')

        print('::group::CRC verification process start')
        crc_val_from_ds = events[-1].extra['crc32']
        file_path = events[-1].extra['file_path']

        with open(file_path) as f:
            contents = f.read()
            crc_of_file = crc32(contents.encode())

        print(f'CRC of ds: {crc_val_from_ds}')
        print(f'CRC of file: {crc_of_file}')
        if crc_of_file == crc_val_from_ds:
            print('CRC verification Success')
        else:
            print('CRC verification Faile.')
        print('::endgroup::')

    task_consumer_with_metadata()