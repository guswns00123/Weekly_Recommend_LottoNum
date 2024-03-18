import pandas as pd
from sklearn.linear_model import LinearRegression


def predict_lotto_num():
    tb = pd.read_csv("/opt/airflow/files/TbLottoStatus/TbLottoStatus.csv")
    tb = tb.drop(columns=['returnValue','drwNoDate'])
    tb = tb[['drwtNo1','drwtNo2','drwtNo3','drwtNo4','drwtNo5','drwtNo6','bnusNo']]
    tb['num'] = 0
    tb = tb.sample(n=700)
    tb.head()


    x = tb['num']
    num_1 = tb['drwtNo1']
    num_2 = tb['drwtNo2']
    num_3 = tb['drwtNo3']
    num_4 = tb['drwtNo4']
    num_5 = tb['drwtNo5']
    num_6 = tb['drwtNo6']
    num_bns = tb['bnusNo']

    pre_num_1 = LinearRegression()
    pre_num_1.fit(x.values.reshape(-1,1),num_1)
    ans_1 = pre_num_1.predict([[len(tb)+1]])[0]

    pre_num_2 = LinearRegression()
    pre_num_2.fit(x.values.reshape(-1,1),num_2)
    ans_2 = pre_num_2.predict([[len(tb)+1]])[0]

    pre_num_3 = LinearRegression()
    pre_num_3.fit(x.values.reshape(-1,1),num_3)
    ans_3 = pre_num_3.predict([[len(tb)+1]])[0]

    pre_num_4 = LinearRegression()
    pre_num_4.fit(x.values.reshape(-1,1),num_4)
    ans_4 = pre_num_4.predict([[len(tb)+1]])[0]

    pre_num_5 = LinearRegression()
    pre_num_5.fit(x.values.reshape(-1,1),num_5)
    ans_5 = pre_num_5.predict([[len(tb)+1]])[0]
    pre_num_6 = LinearRegression()
    pre_num_6.fit(x.values.reshape(-1,1),num_6)
    ans_6 = pre_num_6.predict([[len(tb)+1]])[0]

    pre_num_bns = LinearRegression()
    pre_num_bns.fit(x.values.reshape(-1,1),num_bns)
    ans_bns = pre_num_bns.predict([[len(tb)+1]])[0]

    this_week = []
    this_week.append(int(round(ans_1,0)))
    this_week.append(int(round(ans_2,0)))
    this_week.append(int(round(ans_3,0)))
    this_week.append(int(round(ans_4,0)))
    this_week.append(int(round(ans_5,0)))
    this_week.append(int(round(ans_6,0)))
    this_week.append(int(round(ans_bns,0)))
    print(this_week)
    return this_week