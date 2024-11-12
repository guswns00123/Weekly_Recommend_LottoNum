from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from sklearn.linear_model import LinearRegression


class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            port=self.port,
        )
        return self.postgres_conn

    def select(self, table_name):
        self.log.info("테이블 :" + table_name)
        conn = self.get_conn()
        sql = "select * from lotto_add_table;"
        tb = psql.read_sql(sql, conn)
        tb = tb.drop(columns=["returnValue", "drwNoDate"])
        tb = tb[
            ["drwtNo1", "drwtNo2", "drwtNo3", "drwtNo4", "drwtNo5", "drwtNo6", "bnusNo"]
        ]
        tb["num"] = 0
        tb = tb.sample(n=700)
        tb.head()

        x = tb["num"]
        num_1 = tb["drwtNo1"]
        num_2 = tb["drwtNo2"]
        num_3 = tb["drwtNo3"]
        num_4 = tb["drwtNo4"]
        num_5 = tb["drwtNo5"]
        num_6 = tb["drwtNo6"]
        num_bns = tb["bnusNo"]

        pre_num_1 = LinearRegression()
        pre_num_1.fit(x.values.reshape(-1, 1), num_1)
        ans_1 = pre_num_1.predict([[len(tb) + 1]])[0]

        pre_num_2 = LinearRegression()
        pre_num_2.fit(x.values.reshape(-1, 1), num_2)
        ans_2 = pre_num_2.predict([[len(tb) + 1]])[0]

        pre_num_3 = LinearRegression()
        pre_num_3.fit(x.values.reshape(-1, 1), num_3)
        ans_3 = pre_num_3.predict([[len(tb) + 1]])[0]

        pre_num_4 = LinearRegression()
        pre_num_4.fit(x.values.reshape(-1, 1), num_4)
        ans_4 = pre_num_4.predict([[len(tb) + 1]])[0]

        pre_num_5 = LinearRegression()
        pre_num_5.fit(x.values.reshape(-1, 1), num_5)
        ans_5 = pre_num_5.predict([[len(tb) + 1]])[0]
        pre_num_6 = LinearRegression()
        pre_num_6.fit(x.values.reshape(-1, 1), num_6)
        ans_6 = pre_num_6.predict([[len(tb) + 1]])[0]

        pre_num_bns = LinearRegression()
        pre_num_bns.fit(x.values.reshape(-1, 1), num_bns)
        ans_bns = pre_num_bns.predict([[len(tb) + 1]])[0]

        this_week = []
        this_week.append(int(round(ans_1, 0)))
        this_week.append(int(round(ans_2, 0)))
        this_week.append(int(round(ans_3, 0)))
        this_week.append(int(round(ans_4, 0)))
        this_week.append(int(round(ans_5, 0)))
        this_week.append(int(round(ans_6, 0)))
        this_week.append(int(round(ans_bns, 0)))
        print(this_week)
        return this_week

    def bulk_load(
        self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool
    ):
        from sqlalchemy import create_engine

        self.log.info("적재 대상파일:" + file_name)
        self.log.info("테이블 :" + table_name)
        self.get_conn()
        header = 0 if is_header else None  # is_header = True면 0, False면 None
        if_exists = (
            "replace" if is_replace else "append"
        )  # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace("\r\n", "")  # 줄넘김 및 ^M 제거
                self.log.info(f"{table_name}.{col}: 개행문자 제거")
            except:
                continue

        self.log.info("적재 건수:" + str(len(file_df)))
        uri = f"postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}"
        engine = create_engine(uri)
        file_df.to_sql(
            name=table_name,
            con=engine,
            schema="public",
            if_exists=if_exists,
            index=False,
        )
