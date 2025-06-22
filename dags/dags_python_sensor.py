import pendulum
from airflow.hooks.base import BaseHook
from airflow.sdk import DAG
from airflow.providers.standard.sensors.python import PythonSensor


with DAG(
    dag_id='dags_python_sensor',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule='10 1 * * *',
    catchup=False
) as dag:
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwargs):
        import requests
        import json
        from dateutil import relativedelta
        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100'
        response = requests.get(url)
        
        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10]    # YYYY.MM.DD -> 10 characters
        last_date = last_date.replace('.', '-').replace('/', '-')
        try:
            pendulum.from_format(last_date,'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col} Column is not in YYYY.MM.DD or YYYY/MM/DD format.')

        today_ymd = kwargs.get('data_interval_end').in_timezone('Europe/Berlin').strftime('%Y-%m-%d')
        if last_date >= today_ymd:
            print(f'Created Check (Batch Date: {today_ymd} / API Last Date: {last_date})')
            return True
        else:
            print(f'Update Not Completed (Batch Date: {today_ymd} / API Last Date:{last_date})')
            return False

    sensor_task = PythonSensor(
        task_id='sensor_task',
        python_callable=check_api_update,
        op_kwargs={'http_conn_id':'openapi.seoul.go.kr',
                   'endpoint':'{{var.value.apikey_openapi_seoul_go_kr}}/json/TbCorona19CountStatus',
                   'base_dt_col':'S_DT'},
        poke_interval=600,   #10 minutes
        mode='reschedule'
    )