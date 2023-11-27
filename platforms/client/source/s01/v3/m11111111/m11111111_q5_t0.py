#python code to execute query(.py)
import pymongo
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

def get_data(collection):
  return pd.DataFrame(list(db[collection].find()))

dfs = {
    "nation": get_data('nation'),
    "region": get_data('region'),
    "supplier": get_data('supplier'),
    "customer": get_data('customer'),
    "orders": get_data('orders'),
    "lineitem": get_data('lineitem')
}

dfs['region'] = dfs['region'][dfs['region']['R_NAME'] == 'ASIA']
dfs['orders'] = dfs['orders'][dfs['orders']['O_ORDERDATE'].between('1990-01-01', '1995-01-01')]

# Proceed with pandas merge operations to simulate JOIN (left, right, inner, outer)
# df = dfs['customer'].merge(dfs[....

# ......More pandas manipulations to finish the query.

# And finally write to CSV
# final_df.to_csv('query_output.csv', index=False)
