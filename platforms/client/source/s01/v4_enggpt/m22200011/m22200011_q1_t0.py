from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# query MongoDB for line items with a shipping date on or before '1998-09-02'
filter_query = {
    'L_SHIPDATE': {
        '$lte': datetime(1998, 9, 2)
    }
}

project_query = {
    '_id': 0,
    'L_RETURNFLAG': 1,
    'L_LINESTATUS': 1,
    'L_QUANTITY': 1,
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    'L_TAX': 1
}

cursor = lineitem_collection.find(filter_query, project_query)

# prepare the dataframe
df = pd.DataFrame(list(cursor))

# calculate derived columns and perform aggregation
result = (df
          .assign(SUM_DISC_PRICE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']),
                  SUM_CHARGE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX']))
          .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
          .agg(SUM_QTY=('L_QUANTITY', 'sum'),
               SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
               SUM_DISC_PRICE=('SUM_DISC_PRICE', 'sum'),
               SUM_CHARGE=('SUM_CHARGE', 'sum'),
               AVG_QTY=('L_QUANTITY', 'mean'),
               AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
               AVG_DISC=('L_DISCOUNT', 'mean'),
               COUNT_ORDER=('L_QUANTITY', 'size'))
          .sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], ascending=True)
          .reset_index()
          )

# write the output to a csv file
result.to_csv('query_output.csv', index=False)
