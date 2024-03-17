from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class LottoApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    #www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo=회차번호
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'www.dhlottery.co.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = 'common.do?method=getLottoNumber&drwNo'
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/{self.endpoint}'
        total_row_df = pd.DataFrame()

        start_drwNo = 1
        while True:
            self.log.info(f'시작:{start_drwNo}')
            row_df = self._call_api(self.base_url, start_drwNo)
            print(row_df)
            total_row_df = pd.concat([total_row_df, row_df])
            if start_drwNo == 1110:
                break
            else:
                start_drwNo += 1
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, drwNo):
        import requests
        import json 

        headers={'Content-Type': 'application/json',
                'charset': 'utf-8',
                'Accept': '*/*'}

        request_url = f'{base_url}={drwNo}'
        
        if self.base_dt is not None:
            request_url = f'{base_url}={drwNo}'
        
        response = requests.get(request_url, headers)
        
        contents = json.loads(response.text)
        
        selected_columns = ["drwNoDate", "drwtNo1", "drwtNo2","drwtNo3","drwtNo4","drwtNo5","drwtNo6","bnusNo",'returnValue']  # 원하는 칼럼들의 이름
        selected_values = [contents.get(column_name) for column_name in selected_columns]  # 원하는 칼럼들의 값들

        row_df = pd.DataFrame([selected_values], columns=selected_columns, index = 0)
        

        return row_df