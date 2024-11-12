from operators.lotto_api_to_csv_operator import LottoApiToCsvOperator
from airflow import DAG
from airflow.decorators import task
import pendulum
from datetime import timedelta

default_args = {
    "retries": 3,  # 최대 재시도 횟수
    "retry_delay": timedelta(minutes=5),  # 재시도 간격
    "max_retry_delay": timedelta(minutes=10),  # 최대 재시도 지연 시간
    "retry_exponential_backoff": True,  # 재시도 시 지연 시간을 지수 형태로 증가
}


# 누적 로또 데이터 가져오기
with DAG(
    dag_id="dags_lotto_api",
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 17, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    dag.doc_md = """
    ### DAG 설명
    이 DAG는 일회성 으로 URL을 통해 로또 누적 데이터를 CSV 파일로 저장합니다. 
    매주 최신 로또 데이터를 업데이트하기 위한 기본 데이터로 데이터 파이프라인의 일부로 활용될 수 있습니다.
    """

    @task(task_id="tb_lotto_status")
    def fetch_lotto_data():
        try:
            LottoApiToCsvOperator(
                task_id="tb_lotto_status",
                dataset_nm="TblottoStatus",
                path=f'/opt/airflow/files/TbLottoStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
                file_name="TbLottoStatus.csv",
            ).execute({})
        except Exception as e:
            # 에러 발생 시 로그 출력
            print(f"Error occurred: {e}")
            raise  # 에러를 재발생시켜 재시도 트리거

    tb_lotto_status = fetch_lotto_data()

    tb_lotto_status.doc_md = "Lotto 당첨 번호 누적 데이터를 CSV로 저장하는 작업"
