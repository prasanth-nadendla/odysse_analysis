import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

def transform_df(df,name):
    df.to_csv(os.getenv("base_path")+'/'+str(name)+".csv",header=True,index=False)
    print("file stored in temp file path")
    print(os.getenv('base_path'))
    return str(os.getenv("base_path"))+str(name)