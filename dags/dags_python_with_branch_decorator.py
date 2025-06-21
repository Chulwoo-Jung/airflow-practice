from airflow.sdk import DAG, task
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_branch_decorator',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule='0 1 * * *',
    catchup=False
) as dag:

    @task.branch(task_id='get_random_task')
    def get_random_task():
        import random
        return random.choice(['task_a','task_b','task_c'])
    
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=lambda: print('task_a')
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=lambda: print('task_b')
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=lambda: print('task_c')
    )
    
    get_random_task >> [task_a, task_b, task_c]