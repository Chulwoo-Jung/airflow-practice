from airflow.sdk import DAG
import pendulum
import datetime as dt
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id = 'dags_email_operator',
    schedule = "0 8 1 * *",
    start_date = pendulum.datetime(2025,1,1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'sdfghfwwz@naver.com',
        subject = 'Airflow Email Test',
        html_content = 'Airflow Email Test'
    )

    send_email_task