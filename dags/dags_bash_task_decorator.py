import pendulum
import os
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_task_decorator",
        schedule=None,
        start_date=pendulum.datetime(2025, 6, 1, tz="Europe/Berlin"),
        catchup=False,
        tags=['bash','taskflow']
) as dag:
    
    @task.bash(task_id='task_select_fruit')
    def task_select_fruit():
        return '/opt/airflow/plugins/shell/select_fruit.sh ORANGE'

    @task(task_id='test')
    def sample():
        print('good')

    @task.bash(task_id='task_skip_state')
    def task_skip_state():
        # code 99 -> Skip status 
        return 'echo "This is skip status";exit 99'

    @task.bash(task_id='task_get_env',
               env={'START_DATE':'{{ data_interval_start.in_timezone("Europe/Berlin") | ds }}',
                    'END_DATE':'{{ (data_interval_end.in_timezone("Europe/Berlin") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
                    })
    def task_get_env():
        return 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'

    # Using @task.bash is recommended over BashOperator since it allows writing logic using Python syntax
    # Example) Program that counts number of files in each directory under dags directory
    @task.bash(task_id='task_py_is_better_than_bash_operator')
    def task_py_is_better_than_bash_operator():
        file_cnt_per_dir = {}
        for (path, dir, files) in os.walk("/opt/airflow/dags"):
            if path.endswith('__pycache__'):
                continue
            file_cnt = len(files)
            file_cnt_per_dir[path] = file_cnt
        return f'echo file count in /opt/airflow/dags: {file_cnt_per_dir} '

    task_select_fruit() >> task_skip_state()
    task_get_env()
    task_py_is_better_than_bash_operator()
    sample()