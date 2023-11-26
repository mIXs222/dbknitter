# query.py
from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_key = 'part'
part_data = redis_client.get(part_key)
if part_data:
    redis_part = pd.read_json(part_data, orient='split')

# Processing part
filtered_part = redis_part[
    (redis_part['P_BRAND'] == 'Brand#23') &
    (redis_part['P_CONTAINER'] == 'MED BAG')
]

# SQL-like query in pandas
result = pd.DataFrame(columns=['AVG_YEARLY'])

for index, part_row in filtered_part.iterrows():
    temp_lineitem = mongo_lineitem[mongo_lineitem['L_PARTKEY'] == part_row['P_PARTKEY']]
    avg_quantity = temp_lineitem['L_QUANTITY'].mean()
    
    qualifying_lineitems = temp_lineitem[
        temp_lineitem['L_QUANTITY'] < 0.2 * avg_quantity
    ]
    
    avg_yearly = qualifying_lineitems['L_EXTENDEDPRICE'].sum() / 7.0
    result = result.append({'AVG_YEARLY': avg_yearly}, ignore_index=True)

# Output the result to 'query_output.csv'
result.to_csv('query_output.csv', index=False)
