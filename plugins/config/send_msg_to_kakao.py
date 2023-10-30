from config.kakao_api import send_kakao_msg

def send_success_msg_to_kakao(**kwargs):
    print(kwargs)
    ti = kwargs.get('ti')
    num_list = ti.xcom_pull(task_ids="python_t1")
    dag_id = ti.dag_id
    task_id = ti.task_id
    data_interval_end = kwargs.get('data_interval_end').in_timezone('Asia/Seoul')
    content = {f'{dag_id}.{task_id}': f'추천번호: {num_list}', '':''}      # Content 길이는 2 이상
    send_kakao_msg(talk_title=f'task 이번주 추천 알람({data_interval_end})',
                   content=content)