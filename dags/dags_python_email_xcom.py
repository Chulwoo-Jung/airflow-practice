from airflow.sdk import DAG, task
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum

with DAG(
    dag_id='dags_python_email_xcom',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    @task(task_id='some_logic')
    def some_logic(**kwargs):
        from random import choice
        return choice(['lucky!', 'tmrw will be better'])
    

    send_email = EmailOperator(
        task_id = 'send_email',
        to = '{{var.value.sumin_email}}',
        subject = '{{ data_interval_end.in_timezone("Europe/Berlin") | ds }} : Lucky TEST!! : Will you be lucky today?',
        html_content = '{{ data_interval_end.in_timezone("Europe/Berlin") | ds }} , today <br> \
                        {{ ti.xcom_pull(key="return_value", task_ids="some_logic")}}, \
                            <br> {{var.value.secret_message_to_Sumin}}<br>'
    )

    some_logic() >> send_email