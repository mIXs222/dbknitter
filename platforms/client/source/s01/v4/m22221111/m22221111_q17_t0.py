import pandas as pd
from pymongo import MongoClient
import direct_redis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get lineitem data from MongoDB
lineitem_data = pd.DataFrame(list(lineitem_collection.find(
    {}, {'L_ORDERKEY': 1, 'L_PARTKEY': 1, 'L_QUANTITY': 1, 'L_EXTENDEDPRICE': 1}
)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)

# Get part data from Redis (assuming it's stored as a JSON string)
part_data_json = redis_client.get('part')
part_data = pd.read_json(part_data_json, lines=True)

# SQL query translated to Pandas
# First we filter `part_data` with conditions.
filtered_parts = part_data[
    (part_data['P_BRAND'] == 'Brand#23') &
    (part_data['P_CONTAINER'] == 'MED BAG')
]

# Then we calculate the AVG(L_QUANTITY) for each P_PARTKEY
avg_quantity = lineitem_data.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

filtered_parts = filtered_parts.merge(avg_quantity, left_on='P_PARTKEY', right_on='L_PARTKEY')
filtered_parts['QUANTITY_LIMIT'] = 0.2 * filtered_parts['AVG_QUANTITY']

filtered_lineitems = lineitem_data[lineitem_data['L_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]

# We perform a semi-join and filter `filtered_lineitems` by `filtered_parts`'s conditions.
final_lineitems = filtered_lineitems[
    filtered_lineitems.apply(
        lambda x: x['L_QUANTITY'] < filtered_parts[
            filtered_parts['P_PARTKEY'] == x['L_PARTKEY']
        ]['QUANTITY_LIMIT'].iloc[0], axis=1
    )
]

# Now we sum up all L_EXTENDEDPRICE from `final_lineitems` and divide by 7.0
avg_yearly = final_lineitems['L_EXTENDEDPRICE'].sum() / 7.0
result = pd.DataFrame({'AVG_YEARLY': [avg_yearly]})

# Write the result to query_output.csv
result.to_csv('query_output.csv', index=False)
