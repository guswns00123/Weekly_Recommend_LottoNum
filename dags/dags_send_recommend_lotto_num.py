from airflow import DAG
from airflow.operators.python import PythonOperator
from common.predict import predict_lotto_num
from airflow.operators.email import EmailOperator
from airflow.decorators import task
from airflow import Dataset
from datetime import timedelta
import pendulum
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from config.send_msg_to_kakao import send_success_msg_to_kakao
from hooks.custom_postgres_hook import CustomPostgresHook

dataset_dags_dataset_producer = Dataset("dags_lotto_data")

with DAG(
    dag_id="dags_recommend_lotto_num",
    schedule=[dataset_dags_dataset_producer],
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["lotto", "recommend"],
    default_args={
        "on_failure_callback": on_failure_callback_to_kakao,
        "execution_timeout": timedelta(seconds=180),
    },
) as dag:
    doc_md = """
    # 로또 번호 추천 DAG 설명

    이 DAG는 로또 번호를 추천하는 프로세스를 자동화하며, 추천된 로또 번호를 이메일과 카카오톡으로 전송하는 워크플로입니다.
    """

    @task(
        task_id="inner_function1",
        doc_md="""
    이 태스크는 로또 번호 추천 작업을 시작하며, '로또 번호 추천 작업 시작' 메시지를 로그에 출력합니다.
    초기화 단계로, 다른 태스크들이 실행되기 전에 로깅을 통해 시작을 알립니다.
    """,
    )
    def inner_func1(**kwargs):
        print("로또 번호 추천 작업 시작")

    @task(
        task_id="lotto_num_from_local_db",
        doc_md="""
    이 태스크는 로컬 DB에서 로또 번호를 예측하는 작업입니다.
    `predict_lotto_num` 함수를 실행하여 추천된 로또 번호를 생성합니다.
    예측된 번호는 이메일로 전달됩니다.
    """,
    )
    def predict_lotto_num_task():
        return predict_lotto_num()

    @task(
        task_id="lotto_num_from_postgres_db",
        doc_md="""
    이 태스크는 Postgres DB에서 `lotto_add_table` 테이블을 조회하여 로또 번호 추천에 필요한 데이터를 가져옵니다.
    조회된 데이터는 추천 로또 번호 생성을 위해 사용되어 예측된 번호는 카카오 메시지로 전달됩니다.
    """,
    )
    def select_postgres_task(postgres_conn_id, tbl_nm):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        return custom_postgres_hook.select(table_name=tbl_nm)

    send_num_to_email = EmailOperator(
        task_id="send_email",
        to="fresh0911@naver.com",
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 이번 주 추천 번호는 <br> \
            {{ti.xcom_pull(task_ids="predict_lotto_num")}} 입니다 <br>',
    )

    @task(
        task_id="send_num_to_kakao",
        doc_md="""
    이 태스크는 카카오톡 메시지로 로또 번호를 전송합니다.
    `send_success_msg_to_kakao` 함수를 호출하여 카카오톡 알림을 보냅니다.
    예측된 로또 번호와 함께 성공적인 실행을 알리는 메시지를 전송합니다.
    """,
    )
    def send_num_to_kakao_task():
        send_success_msg_to_kakao()

    # Task dependencies following the specified chain rules
    inner_function1_instance = inner_func1()
    predict_lotto_num_instance = predict_lotto_num_task()
    select_postgres_instance = select_postgres_task(
        "conn-db-postgres-custom", "lotto_add_table"
    )

    # Set task dependencies
    inner_function1_instance >> [predict_lotto_num_instance, select_postgres_instance]
    predict_lotto_num_instance >> send_num_to_email
    select_postgres_instance >> send_num_to_kakao_task()
