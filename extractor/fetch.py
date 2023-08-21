from data_loading import Combiner

df = Combiner(sql_files=["C:\\Users\\cherukuri.rohan\\Desktop\\extractor\\CLAIM_REJECTION.sql"], 
              paths=["C:\\Users\\cherukuri.rohan\\Desktop\\extractor\\CLAIM_REJECTION.xlsx"], 
              server="bi_da", 
              format_type="excel", password=False).execute().collect()[0]
print(df)