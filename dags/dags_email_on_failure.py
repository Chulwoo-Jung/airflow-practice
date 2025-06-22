from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import pendulum
from datetime import timedelta
from airflow.sdk import DAG, task, Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('error occurred')
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
        execution_timeout=timedelta(seconds=10)
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
        execution_timeout=timedelta(seconds=10)
    )