import pymongo
import pandas as pd
from pandas.io.json import json_normalize

client = pymongo.MongoClient("mongodb://mongodb:27017/")
database = client["tpch"]
customer = database["customer"]
orders = database["orders"]

customer_data = pd.DataFrame(list(customer.find()))
orders_data = pd.DataFrame(list(orders.find()))

merged_data = pd.merge(customer_data, orders_data, how='left', 
                       left_on='C_CUSTKEY', right_on='O_CUSTKEY')

merged_data['O_COMMENT'] = merged_data['O_COMMENT'].apply(
    lambda x: None if 'pending deposits' in str(x) else x)

grouped_data = merged_data.groupby('C_CUSTKEY').size().reset_index(name='C_COUNT')
grouped_data = grouped_data.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

result = grouped_data.sort_values(['CUSTDIST', 'C_COUNT'], ascending=[False, False])

result.to_csv('query_output.csv', index=False)
