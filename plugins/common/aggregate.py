import pandas as pd
import glob
import os
def merge_excel_files(file_path, file_format, save_path, save_format, columns=None):
    merge_df = pd.DataFrame()
    
    file_list = glob.glob(f"{file_path}/**/*{file_format[0]}", recursive=True)
    file_list2 = glob.glob(f"{file_path}/**/*{file_format[1]}", recursive=True)
    for file in file_list:
        if file_format[0] == ".xls":
            file_df = pd.read_excel(file,header=None)

        
        columns = file_df.columns
            
        temp_df = pd.DataFrame(file_df, columns=columns)
        
        merge_df = merge_df.append(temp_df)
    
    for file in file_list2:
        if file_format[1] == ".xlsx":
            file_df = pd.read_excel(file,header=None)

        
        columns = file_df.columns
            
        temp_df = pd.DataFrame(file_df, columns=columns)
        
        merge_df = merge_df.append(temp_df)

    if save_format == ".xlsx":
        merge_df.to_excel(save_path, index=False)
    else:
        merge_df.to_csv(save_path, index=False)
        

if __name__ == "__main__":
    merge_excel_files(file_path="C:/Users/joon0/data/21/현대", file_format=[".xls",".xlsx"], 
                      save_path="C:/Users/joon0/data/21/현대/병합.xlsx", save_format=".xlsx")
