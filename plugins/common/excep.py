# import pandas as pd
# import glob

# filename = "C:/Users/joon0/data/21/현대/병합.xlsx"

# # 엑셀 파일 읽어 오기
# df = pd.read_excel(filename, engine='openpyxl')

# dataFilter = df[4].str.contains("010", na=False)




# res = df[dataFilter]
# res.columns = [i for i in range(10)]
# # res.columns = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]
# # res.columns = [0,1,2,3,4,5,6,7,8,9]
# print(res.drop_duplicates([4,5]))
# res = res.drop_duplicates([4,5])
# res.to_excel("C:/Users/joon0/data/21/현대/check2.xlsx", index=False)
    
