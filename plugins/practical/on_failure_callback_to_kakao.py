from practical.kakao_api import send_kakao_msg

def on_failure_callback_to_kakao(context):
    exception = context.get('exception') or 'no exception'
    ti = context.get('ti')
    dag_id = ti.dag_id
    task_id = ti.task_id
    data_interval_end = context.get('data_interval_end').in_timezone('Europe/Berlin')

    content = {f'{dag_id}.{task_id}': f'Error Message: {exception}', '':''}      # Content length must be 2 or more
    send_kakao_msg(talk_title=f'task failure alert({data_interval_end})',
                   content=content)