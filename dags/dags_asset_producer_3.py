import pendulum
from airflow.sdk import DAG, task, Asset

asset_dags_asset_producer_3 = Asset("dags_asset_producer_3")

with DAG(
        dag_id='dags_asset_producer_3',
        schedule=None,
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        catchup=False,
        tags=['dataset','producer']
) as dag:
    @task(task_id='task_producer_3',
          outlets=[asset_dags_asset_producer_3])
    def task_producer_3():
        print('dataset update complete')

    task_producer_3()