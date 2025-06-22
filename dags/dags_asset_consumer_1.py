import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset


asset_dags_asset_producer_1 = Asset("dags_asset_producer_1")

with DAG(
        dag_id='dags_asset_consumer_1',
        schedule=[asset_dags_asset_producer_1], # subscribe to the Asset("dags_asset_producer_1")
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        catchup=False,
        tags=['asset','consumer']
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1 completed"'
    )