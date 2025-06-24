from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum
from contextlib import closing
import pandas as pd
from airflow.sdk import DAG, Variable, task



email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_daily_dag_monitoring_to_email',
    start_date=pendulum.datetime(2025,6,24, tz='Europe/Berlin'),
    schedule='0 8 * * *',
    catchup=False
) as dag:
    
    @task(task_id='get_daily_monitoring_rslt_task')
    def get_daily_monitoring_rslt_task(**kwargs):
        postgres_hook = PostgresHook(postgres_conn_id='conn-db-postgres-airflow')
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/files/sql/airflow_dag_monitoring_query.sql', 'r') as sql_file:
                    cursor.execute("SET TIME ZONE 'Europe/Berlin';")
                    sql = '\n'.join(sql_file.readlines())
                    cursor.execute(sql)
                    rslt = cursor.fetchall()
                    rslt = pd.DataFrame(rslt)
                    rslt.columns = ['dag_id','run_cnt','success_cnt','failed_cnt','running_cnt','last_failed_date','last_success_date','next_dagrun_data_interval_start','next_dagrun_data_interval_end']
                    html_content = ''

                    # 1) Failure target
                    failed_df = rslt[rslt['failed_cnt'] > 0]
                    html_content += "<h2>2 Failure target</h2><br/>"
                    if not failed_df.empty:
                        for idx, row in failed_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>Latest failure date: {row['last_failed_date']}<br/>Latest success date: {'N/A' if str(row['last_success_date']) =='NaT' else row['last_success_date']}<br/><br/>"
                    else:
                        html_content += "N/A<br/><br/>"


                    # 2) Skipped target
                    skipped_df = rslt[rslt['run_cnt'] == 0]
                    html_content += "<h2>3 Skipped target</h2><br/>"
                    if not skipped_df.empty:
                        for idx, row in skipped_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>Next execution date: {row['next_dagrun_data_interval_end']}<br/><br/>"
                    else:
                        html_content += "N/A<br/><br/>"

                    # 3) Running target
                    running_df = rslt[rslt['running_cnt'] > 0]
                    html_content += "<h2>4 Running target</h2><br/>"
                    if not running_df.empty:
                        for idx, row in running_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>Next execution date: {row['next_dagrun_data_interval_start']}<br/><br/>"
                    else:
                        html_content += "N/A<br/><br/>"

                    # 4) Success target
                    done_success_cnt = rslt[(rslt['failed_cnt'] == 0) & (rslt['run_cnt'] > 0) & (rslt['running_cnt'] == 0)].shape[0]
                    yesterday = pendulum.yesterday('Europe/Berlin').strftime('%Y-%m-%d')
                    now = pendulum.now('Europe/Berlin').strftime('%Y-%m-%d %H:%M:%S')

                    ti = kwargs['ti']
                    ti.xcom_push(key='subject', value=f"DAG execution status alert ({yesterday} ~ {now})")
                    html_content = f'''<h1>DAG execution status alert ({yesterday} ~ {now})</h1><br/><br/>
<h2>1. Number of DAGs to be executed: {rslt.shape[0]}</h2><br/>
&nbsp;&nbsp;&nbsp;&nbsp;(1) Number of successful DAGs: {done_success_cnt}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(2) Failure: {failed_df.shape[0]}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(3) Skipped: {skipped_df.shape[0]}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(4) Running: {running_df.shape[0]}<br/><br/>''' + html_content
                    
                    print(html_content)
                    return html_content

    send_email = EmailOperator(
        task_id='send_email',
        conn_id='conn_smtp_gmail',
        to=email_lst,
        subject="{{ti.xcom_pull(key='subject')}}",
        html_content="{{ti.xcom_pull(key='return_value')}}"
    )

    get_daily_monitoring_rslt_task() >> send_email