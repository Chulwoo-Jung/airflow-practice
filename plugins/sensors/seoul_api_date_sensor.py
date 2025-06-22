from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

"""
This sensor checks if the data for the previous day exists in the API.

This Sensor is for the Descending order of the API.
"""
class SeoulApiDateSensor(BaseSensorOperator):
    template_fields = ('endpoint',)
    def __init__(self, dataset_nm, base_dt_col, day_off=0, **kwargs):
        '''
        dataset_nm: The name of the dataset to be sensed from the Seoul Open Data Portal
        base_dt_col: The column to be sensed (yyyy.mm.dd... or yyyy/mm/dd... only)
        day_off: The difference in days from the batch date to check for creation (default: 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm + '/1/100'   # Extract only 100 rows
        self.base_dt_col = base_dt_col
        self.day_off = day_off

        
    def poke(self, context):
        import requests
        import json
        from dateutil.relativedelta import relativedelta
        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        self.log.info(f'request url:{url}')
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(self.base_dt_col)
        last_date = last_dt[:8]

        search_ymd = (context.get('data_interval_end').in_timezone('Europe/Berlin') + relativedelta(days=self.day_off)).strftime('%Y%m%d')

        # Check if the last date is in the correct format (YYYYMMDD)
        try:
            import pendulum
            pendulum.from_format(last_date, 'YYYYMMDD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{self.base_dt_col} Column is not in YYYYMMDD format.')


        if last_date >= search_ymd:
            self.log.info(f'Created Check (Reference Date: {search_ymd} / API Last Date: {last_date})')
            return True
        else:
            self.log.info(f'Update Not Completed (Reference Date: {search_ymd} / API Last Date:{last_date})')
            return False