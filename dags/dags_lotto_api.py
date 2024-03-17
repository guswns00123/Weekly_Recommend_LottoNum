from operators.lotto_api_to_csv_operator import LottoApiToCsvOperator
from airflow import DAG
import pendulum


# 누적 로또 데이터 가져오기
with DAG(
    dag_id='dags_lotto_api',
    schedule=None,
    start_date=pendulum.datetime(2024,3,17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    tb_lotto_status = LottoApiToCsvOperator(
        task_id='tb_lotto_status',
        dataset_nm='TblottoStatus',
        path='/opt/airflow/files/TbLottoStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbLottoStatus.csv'
    )

    tb_lotto_status