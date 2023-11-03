from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://mongodb:27017')
db = client['tpch']

pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {
        '$match': {
            'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'}
        }
    }
]

orders = list(db.orders.aggregate(pipeline))

#Convert orders to a DataFrame and flatten the lineitems.
df_orders = pd.json_normalize(orders, 'lineitems', ['O_ORDERPRIORITY'], 
            record_prefix='L_', errors='ignore')

#Filter df_orders where L_COMMITDATE < L_RECEIPTDATE
df_orders = df_orders.loc[df_orders['L_COMMITDATE'] < df_orders['L_RECEIPTDATE']]

df_count = df_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

df_count.sort_values('O_ORDERPRIORITY').to_csv('query_output.csv', index=False)
