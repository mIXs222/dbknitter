# Python code saved as execute_query.py

from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# MongoDB Connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Fetch lineitem data from MongoDB
pipeline = [
    {
        '$match': {
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'L_COMMITDATE': {'$lte': '2024-09-01'},
            'L_RECEIPTDATE': {'$gte': '2020-09-01'}
        }
    }
]
lineitem_df = pd.DataFrame(list(lineitem_collection.aggregate(pipeline)))

# Redis Connection using DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch part data from Redis and convert to DataFrame
part_str = redis_client.get('part')
part_df = pd.read_json(part_str)

# Filter parts with BRAND#23 and MED BAG
filtered_parts = part_df[(part_df['P_BRAND'] == 'BRAND#23') &
                         (part_df['P_CONTAINER'] == 'MED BAG')]

# Merge filtered parts with lineitem
merged_df = pd.merge(lineitem_df, filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate average lineitem quantity of such parts ordered
average_quantity = merged_df['L_QUANTITY'].mean()

# Determine loss in revenue for parts with quantity less than 20% of this average
loss_df = merged_df[merged_df['L_QUANTITY'] < 0.2 * average_quantity]
loss_df['LOSS'] = loss_df['L_EXTENDEDPRICE'] * (1 - loss_df['L_DISCOUNT']) * loss_df['L_QUANTITY']

# Calculate and save average yearly loss
average_yearly_loss = loss_df['LOSS'].sum() / 7
result_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
result_df.to_csv('query_output.csv', index=False)

print("The query has been executed and the output is written to query_output.csv.")
