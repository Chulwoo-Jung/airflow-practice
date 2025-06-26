from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
from airflow.sdk import DAG, task, Asset
from airflow.timetables.assets import AssetOrTimeSchedule

with DAG(
        dag_id='dags_asset_time_n_asset',
        schedule=AssetOrTimeSchedule(
            timetable=CronTriggerTimetable("* * * * *", timezone="Asia/Seoul"),
            assets=(Asset('dags_asset_producer_3') &
                      (Asset("dags_asset_producer_1") | Asset("dags_asset_producer_2"))
            )
        ),
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        catchup=False,
        tags=['asset','consumer']
) as dag:
    @task.bash(task_id='task_bash')
    def task_bash():
        return 'echo "schedule run"'

    task_bash()