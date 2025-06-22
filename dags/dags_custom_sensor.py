from sensors.seoul_api_date_sensor import SeoulApiDateSensor
import pendulum
from airflow.sdk import DAG

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    schedule=None,
    catchup=False
) as dag:
    tb_corona_19_count_status_sensor = SeoulApiDateSensor(
        task_id='tb_corona_19_count_status_sensor',
        dataset_nm='TbCorona19CountStatus',
        base_dt_col='S_DT',
        year_off=-3,
        poke_interval=600,
        mode='reschedule'
    )
    
    tv_corona19_vaccine_stat_new_sensor = SeoulApiDateSensor(
        task_id='tv_corona19_vaccine_stat_new_sensor',
        dataset_nm='tvCorona19VaccinestatNew',
        base_dt_col='S_VC_DT',
        year_off=-2,
        poke_interval=600,
        mode='reschedule'
    )