from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import practical.slack_block_builder as sb
import pendulum
from contextlib import closing
import pandas as pd
from airflow import DAG


with DAG(
    dag_id='dags_daily_dag_monitoring',
    start_date=pendulum.datetime(2025,6,24, tz='Europe/Berlin'),
    schedule='0 8 * * *',
    catchup=False
) as dag:
    
    @task(task_id='get_daily_monitoring_rslt_task')
    def get_daily_monitoring_rslt_task():
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
                    return_blocks = []

                    # 1) failure target
                    failed_df = rslt.query("(failed_cnt > 0)")
                    return_blocks.append(sb.section_text("*2 Failure target*"))
                    if not failed_df.empty:
                        for idx, row in failed_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*Latest failure date:* {row['last_failed_date']}\n*Latest success date:* {'N/A' if str(row['last_success_date']) =='NaT' else row['last_success_date']}"))
                    else:
                        return_blocks.append(sb.section_text("N/A"))
                    return_blocks.append(sb.divider())

                    # 2) Skipped target
                    skipped_df = rslt.query("(run_cnt == 0)")
                    return_blocks.append(sb.section_text("*3 Skipped target*"))
                    if not skipped_df.empty:
                        for idx, row in skipped_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*Next execution date:* {row['next_dagrun_data_interval_end']}"))
                    else:
                        return_blocks.append(sb.section_text("N/A"))
                    return_blocks.append(sb.divider())

                    # 3) Running target
                    running_df = rslt.query("(running_cnt > 0)")
                    return_blocks.append(sb.section_text("*4 Running target*"))
                    if not running_df.empty:
                        for idx, row in running_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*Next execution date:* {row['next_dagrun_data_interval_start']}"))
                    else:
                        return_blocks.append(sb.section_text("N/A"))
                    return_blocks.append(sb.divider())

                    # 4) Success target
                    done_success_cnt = rslt.query("(failed_cnt == 0) and (run_cnt > 0) and (running_cnt == 0)").shape[0]
                    yesterday = pendulum.yesterday('Asia/Seoul').strftime('%Y-%m-%d')
                    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
                    return_blocks = [ sb.section_text(f"DAG execution status alert ({yesterday} ~ {now})"),
                                      sb.divider(),
                                      sb.section_text(f"*1. Number of DAGs to be executed*: {rslt.shape[0]}\n    (1) Number of successful DAGs: {done_success_cnt}\n    (2) Failure: {failed_df.shape[0]}\n    (3) Skipped: {skipped_df.shape[0]}\n    (4) Running: {running_df.shape[0]}"),
                                      sb.divider()
                    ] + return_blocks
                    return return_blocks

    send_to_slack = SlackWebhookOperator(
        task_id='send_to_slack',
        slack_webhook_conn_id='conn_slack_airflow_bot',
        blocks='{{ ti.xcom_pull(task_ids="get_daily_monitoring_rslt_task") }}'
    )

    get_daily_monitoring_rslt_task() >> send_to_slack