from operators.lotto_api_to_csv_operator import LottoApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_lotto_api',
    schedule=None,
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    tb_lotto_status = LottoApiToCsvOperator(
        task_id='tb_lotto_status',
        dataset_nm='TblottoStatus',
        path='/opt/airflow/files/TbLottoStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbLottoStatus.csv'
    )

    tb_lotto_status