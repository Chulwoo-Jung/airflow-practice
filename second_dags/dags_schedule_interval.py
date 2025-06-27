from airflow.timetables.trigger import CronTriggerTimetable, MultipleCronTriggerTimetable
from airflow.sdk import DAG, task
from datetime import timedelta
from pprint import pprint

with DAG(
    dag_id='dags_cron_trigger_timetable',
    schedule=MultipleCronTriggerTimetable(
        '*/5 1 * * *',
        "*/5 2 * * *",
        timezone='Asia/Seoul',
        interval=timedelta(minutes=5)
    ),
    tags=['cron']
) as dag:
  
    @task(task_id='task_show_context1')
    def task_show_context1(**context):
        print('::group::show context variables')
        pprint(context)
        print('::endgroup::')

    task_show_context1()