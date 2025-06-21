from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
import pendulum

with DAG(
    dag_id='dags_http_operator',
    schedule=None,
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    ''' Seoul Public Bicycle Rent Use Day Info '''

    tb_bicycle_rent_use_day = HttpOperator(
        task_id='tb_bicycle_rent_use_day',
        http_conn_id='openapi.seoul.go.kr', # connection in Airflow UI
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleRentUseDay/1/10/20250101',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_bicycle_rent_use_day')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_bicycle_rent_use_day >> python_2()