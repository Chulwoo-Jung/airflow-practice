from sensors.seoul_api_date_sensor import SeoulApiDateSensor
import pendulum
from airflow.sdk import DAG

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule=None,
    catchup=False
) as dag:
    ListExpenditureInfo = SeoulApiDateSensor(
        task_id='ListExpenditureInfo',
        dataset_nm='ListExpenditureInfo',
        base_dt_col='PAY_YMD',
        day_off=-1,
        poke_interval=600,
        mode='reschedule'
    )
    
    ListExpenditureByDay = SeoulApiDateSensor(
        task_id='ListExpenditureByDay',
        dataset_nm='ListExpenditureByDay',
        base_dt_col='PAY_YMD',
        day_off=-2,
        poke_interval=600,
        mode='reschedule'
    )