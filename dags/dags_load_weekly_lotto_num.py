from airflow import DAG, Dataset, settings
from airflow.decorators import task
from airflow.utils.state import State
from airflow.models import DagRun
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from datetime import timedelta, datetime
import pendulum
from operators.lotto_api_add_csv_operator import LottoApiAddCsvOperator
import psutil
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

dataset_dags_dataset_producer = Dataset("dags_lotto_data")

default_args = {
    "on_failure_callback": on_failure_callback_to_kakao,
    # "execution_timeout": timedelta(seconds=180),
    # "retries": 3,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dags_lotto_data",
    schedule="0 0 * * 6",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args=default_args,
) as dag:
    dag.doc_md = """
    ### DAG 설명
    이 DAG는 Lotto API를 통해 주간 로또 데이터를 CSV 파일에 추가하고 이를 PostgreSQL에 로드한 후, AWS S3에 업로드하는 작업을 수행합니다. 
    매주 최신 로또 데이터를 업데이트해주는 DAG입니다.
    """

    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "Starting weekly lotto data addition..."',
    )

    start_task.doc_md = """
    #### 시작 태스크
    로또 데이터 파이프라인 시작을 알리는 BashOperator입니다. 주간 로또 데이터 처리를 시작할 때 실행됩니다.
    """

    tb_lotto_add = LottoApiAddCsvOperator(
        task_id="tb_lotto_add",
        outlets=[dataset_dags_dataset_producer],
        path='/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name="TbLottoStatus.csv",
        file="/opt/airflow/files/TbLottoStatus/TbLottoStatus.csv",
        time='{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
    )
    tb_lotto_add.doc_md = """
    #### Lotto URL 데이터 CSV 저장
    Lotto URL로부터 최신 로또 데이터를 가져와 CSV 파일로 저장하는 작업입니다. 이 파일은 후속 작업에서 사용.
    """

    @task(outlets=[dataset_dags_dataset_producer], task_id="insrt_postgres")
    def insrt_postgres(
        postgres_conn_id="conn-db-postgres-custom",
        tbl_nm="lotto_add_table",
        file_nm=None,
    ):
        """
        저장된 CSV 파일을 PostgreSQL 데이터베이스에 로드하는 함수입니다.
        """
        from hooks.custom_postgres_hook_lotto import CustomPostgresHookLotto

        file_path = (
            file_nm
            or '/opt/airflow/files/TbLottoAdd/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbLottoStatus.csv'
        )

        custom_postgres_hook = CustomPostgresHookLotto(
            postgres_conn_id=postgres_conn_id
        )
        custom_postgres_hook.bulk_load(
            table_name=tbl_nm,
            file_name=file_path,
            delimiter=",",
            is_header=True,
            is_replace=True,
        )

    insrt_postgres.doc_md = """
    #### PostgreSQL 데이터베이스로 로드
    CSV 파일에 저장된 로또 데이터를 PostgreSQL 데이터베이스에 로드하여 데이터베이스에 기록하는 작업입니다.
    """

    @task(task_id="upload_s3")
    def upload_to_s3(filename=None, key=None, bucket_name="morzibucket"):
        s3_hook = S3Hook("aws_default")
        file_path = (
            filename
            or '/opt/airflow/files/TbLottoAdd/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbLottoStatus.csv'
        )
        s3_key = (
            key
            or 'lotto/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbLottoStatus.csv'
        )

        s3_hook.load_file(
            filename=file_path, key=s3_key, bucket_name=bucket_name, replace=True
        )

    upload_to_s3.doc_md = """
    #### AWS S3 업로드
    로또 데이터가 저장된 CSV 파일을 AWS S3에 업로드하는 작업입니다. 이를 통해 데이터를 백업하거나 다른 서비스와 공유할 수 있습니다.
    """

    finish_task = BashOperator(
        task_id="finish_task",
        bash_command='echo "Weekly lotto data addition complete"',
        outlets=[dataset_dags_dataset_producer],
    )
    finish_task.doc_md = """
    #### 완료 태스크
    주간 로또 데이터 파이프라인이 완료되었음을 알리는 BashOperator입니다. 모든 데이터 처리가 완료된 후 실행됩니다.
    """

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def generate_dag_report():
        """
        지난 30일 동안의 DAG 실행 결과를 기반으로 리포트를 생성하는 태스크.
        성공률, 실패율, 평균 실행 시간, CPU 사용량 및 메모리 사용량을 포함한 종합 리포트를 작성합니다.
        """
        session = settings.Session()
        report_data = {}

        # 지난 30일 동안의 DAG 실행 데이터 수집
        now = pendulum.now("Asia/Seoul")
        thirty_days_ago = now.subtract(days=30)

        # session에서 30일 이내의 DagRun을 가져옵니다.
        dag_runs = (
            session.query(DagRun).filter(DagRun.execution_date >= thirty_days_ago).all()
        )

        # 성공률, 실패율, 평균 실행 시간, CPU 및 메모리 사용량 계산
        for dag_run in dag_runs:
            dag_id = dag_run.dag_id
            if dag_id not in report_data:
                report_data[dag_id] = {
                    "success": 0,
                    "failed": 0,
                    "total_runtime": 0,
                    "count": 0,
                    "cpu_usage": 0,
                    "memory_usage": 0,
                }

            # 성공/실패 횟수 기록
            if dag_run.state == State.SUCCESS:
                report_data[dag_id]["success"] += 1
            elif dag_run.state == State.FAILED:
                report_data[dag_id]["failed"] += 1

            # 실행 시간 합산
            if dag_run.start_date and dag_run.end_date:
                report_data[dag_id]["total_runtime"] += (
                    dag_run.end_date - dag_run.start_date
                ).total_seconds()
            report_data[dag_id]["count"] += 1

            # CPU와 메모리 사용량 측정
            report_data[dag_id]["cpu_usage"] += psutil.cpu_percent()
            report_data[dag_id]["memory_usage"] += psutil.virtual_memory().percent

        report_message = "*Weekly DAG Report*\n"
        for dag_id, stats in report_data.items():
            success_rate = stats["success"] / stats["count"] * 100
            average_runtime = stats["total_runtime"] / stats["count"]
            average_cpu_usage = stats["cpu_usage"] / stats["count"]
            average_memory_usage = stats["memory_usage"] / stats["count"]
            report_message += (
                f"DAG `{dag_id}`:\n"
                f"  - Success Rate: {success_rate:.2f}%\n"
                f"  - Average Runtime: {average_runtime:.2f} sec\n"
                f"  - Average CPU Usage: {average_cpu_usage:.2f}%\n"
                f"  - Average Memory Usage: {average_memory_usage:.2f}%\n\n"
            )

        return report_message

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def send_email(report_content):
        email_task = EmailOperator(
            task_id="send_report_email",
            to="fresh0911@naver.com",
            subject="Weekly Airflow DAG Report",
            html_content=report_content,
        )
        email_task.execute(context={})

    start_task >> tb_lotto_add >> [insrt_postgres(), upload_to_s3()] >> finish_task
    report = generate_dag_report()
    # 상태 리포트는 finish_task 후에 실행
    finish_task >> report >> send_email(report)
