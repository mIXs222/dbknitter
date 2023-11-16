from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd

# connect to mongodb
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
collection = db["lineitem"]

# get the data
data = list(collection.find({"L_SHIPDATE": {"$lte": '1998-09-02'}}))

# create a pandas dataframe
df = DataFrame(data)

# do the operations
result = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=('L_QUANTITY', 'sum'),
    SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
    SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x*(1-df['L_DISCOUNT'])).sum()),
    SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x*(1-df['L_DISCOUNT'])*(1+df['L_TAX'])).sum()),
    AVG_QTY=('L_QUANTITY', 'mean'),
    AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISC=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', 'count')
)

# sort the result
result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)

# save to csv
result.to_csv('query_output.csv')
