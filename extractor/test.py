import pandas as pd

df = pd.read_parquet("C:\\Users\\cherukuri.rohan\\Desktop\\extractor\\config.parquet", engine="pyarrow")

print(df)