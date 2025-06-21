import pendulum
from airflow.sdk import DAG, task, task_group, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_task_group',
    schedule=None,
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    catchup=False
) as dag:

    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)
    
    @task_group(group_id='group_1')
    def group_1():
        '''First group created by task_group decorator'''

        @task(task_id='inner_func1')
        def inner_func1(**kwargs):
            print('First task of First group')
        
        inner_func2 = PythonOperator(
            task_id='inner_func2',
            python_callable=inner_func,
            op_kwargs={'msg':'Second task of First group'}
        )

        inner_func1() >> inner_func2
    

    with TaskGroup(group_id='group_2', tooltip='Second group created by TaskGroup') as group_2:
        
        @task(task_id='inner_func1')
        def inner_func1(**kwargs):
            print('First task of Second group')

        inner_func2 = PythonOperator(
            task_id='inner_func2',
            python_callable=inner_func,
            op_kwargs={'msg':'Second task of Second group'}
        )

        inner_func1() >> inner_func2
    
    group_1() >> group_2