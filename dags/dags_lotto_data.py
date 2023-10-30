from operators.lotto_api_add_csv_operator import LottoApiAddCsvOperator
from airflow import DAG
import pendulum
from airflow import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook
from datetime import timedelta
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


dataset_dags_dataset_producer = Dataset("dags_lotto_data")

with DAG(
    dag_id='dags_lotto_data',
    schedule='0 0 * * 6',
    start_date=pendulum.datetime(2023,10,1, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=180)
    }
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        outlets=[dataset_dags_dataset_producer],
        bash_command='echo "전 주 데이터 추가 작업 시작"'
    )

    tb_lotto_add = LottoApiAddCsvOperator(
        task_id='tb_lotto_add',
        outlets=[dataset_dags_dataset_producer],
        path='/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbLottoStatus.csv',
        file = '/opt/airflow/files/TbLottoStatus/TbLottoStatus.csv',
        time = '{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}'

    )

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgresdb = PythonOperator(
        task_id='insrt_postgres',
        outlets=[dataset_dags_dataset_producer],
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'lotto_add_table',
                   'file_nm':'/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbLottoStatus.csv'}
    )

    def upload_to_s3(filename, key, bucket_name):
        hook = S3Hook('aws_default')
        hook.load_file(filename=filename,
                       key = key,
                       bucket_name=bucket_name,
                       replace=True)
    
    upload_s3 = PythonOperator(
        task_id = 'upload_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename' : '/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbLottoStatus.csv',
            'key' : 'lotto/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbLottoStatus.csv',
            'bucket_name' : 'morzibucket'
        })
    


    finish_task = BashOperator(
        task_id='finish_task',
        outlets=[dataset_dags_dataset_producer],
        bash_command='echo "전 주 데이터 추가 작업 완료"'
    )

    start_task >> tb_lotto_add >> [insrt_postgresdb, upload_s3] >> finish_task