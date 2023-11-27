# query.py
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379)

# Load lineitem from Redis
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Load part from MongoDB and convert to DataFrame
part_docs = part_collection.find()
part_df = pd.DataFrame(list(part_docs))

# Perform the query by filtering data separately and then performing a merge (like a SQL join)
conditions = [
    # CONDITION 1
    (
        (part_df['P_BRAND'] == 'Brand#12') &
        (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
        (part_df['P_SIZE'].between(1, 5))
    ),
    # CONDITION 2
    (
        (part_df['P_BRAND'] == 'Brand#23') &
        (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
        (part_df['P_SIZE'].between(1, 10))
    ),
    # CONDITION 3
    (
        (part_df['P_BRAND'] == 'Brand#34') &
        (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
        (part_df['P_SIZE'].between(1, 15))
    )
]

# Apply conditions to part_df
filtered_parts = part_df[conditions[0] | conditions[1] | conditions[2]]

# Convert Redis data keys to the proper type and names
lineitem_df = lineitem_df.applymap(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
lineitem_df.columns = [col.decode('utf-8') if isinstance(col, bytes) else col for col in lineitem_df.columns]
lineitem_df = lineitem_df.rename(columns=lambda x: f"L_{x}")

lineitem_filtered = lineitem_df[
    lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') &
    (lineitem_df['L_QUANTITY'] >= 1) &
    (lineitem_df['L_QUANTITY'] <= 30)  # The max quantity from OR conditions
]

# Merging the filtered data
result = pd.merge(filtered_parts, lineitem_filtered, left_on='P_PARTKEY', right_on='L_PARTKEY', how='inner')

# Calculate the revenue
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Summing up the revenue
revenue_sum = result['REVENUE'].sum()

# Output the result to a csv file
output_df = pd.DataFrame({'REVENUE': [revenue_sum]})
output_df.to_csv('query_output.csv', index=False)
