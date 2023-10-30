

def select_postgres(postgres_conn_id, **kwargs):
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from contextlib import closing
            
            postgres_hook = PostgresHook(postgres_conn_id)
            with closing(postgres_hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    dag_id = kwargs.get('ti').dag_id
                    task_id = kwargs.get('ti').task_id
                    run_id = kwargs.get('ti').run_id
                    msg = 'hook select 수행'
                    sql = 'select * from lotto_add_table;'
                    cursor.execute(sql, (dag_id, task_id, run_id, msg))
                    rows =cursor.fetchall()
                    print(rows)
                    conn.commit()