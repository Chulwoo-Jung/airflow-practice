from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.branch import BaseBranchOperator
import pendulum


with DAG(
    dag_id='dags_base_branch_operator',
    schedule=None,
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    with DAG(
        dag_id='dags_base_branch_operator_subdag',
        start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
        catchup=False
    ) as dag:
        
        class CustomBranchOperator(BaseBranchOperator):
            def choose_branch(self, context):
                import random 
                print(context)

                item_list = ['A','B','C']
                selected_item = random.choice(item_list)
                if selected_item == 'A':
                    return 'task_a'
                elif selected_item in ['B','C']:
                    return ['task_b','task_c']
                
        custom_branch_task = CustomBranchOperator(
            task_id='custom_branch_task'
        )

        def common_func(**kwargs):
            print(kwargs['selected'])

        task_a = PythonOperator(
            task_id='task_a',
            python_callable=common_func,
            op_kwargs={'selected':'A'}
        )

        task_b = PythonOperator(
            task_id='task_b',
            python_callable=common_func,
            op_kwargs={'selected':'B'}
        )
        
        task_c = PythonOperator(
            task_id='task_c',
            python_callable=common_func,
            op_kwargs={'selected':'C'}
        )

        custom_branch_task >> [task_a, task_b, task_c]
