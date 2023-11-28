import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_lineitem_collection = mongo_db["lineitem"]

# Retrieve data from MongoDB's lineitem table
lineitem_df = pd.DataFrame(list(mongo_lineitem_collection.find()))

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_pandas_bytes = redis_client.get('part')
part_df = pd.read_msgpack(part_pandas_bytes)

# Apply query logic
conditions = [
    (part_df['P_BRAND'] == 'Brand#12') & 
    (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & 
    (lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11) & 
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 5) & 
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (part_df['P_BRAND'] == 'Brand#23') & 
    (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & 
    (lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20) & 
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 10) & 
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (part_df['P_BRAND'] == 'Brand#34') & 
    (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & 
    (lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30) & 
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 15) & 
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & 
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
]

# Merge dataframes on part keys to filter relevant data
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply conditions to the merged dataframe
selected_data = pd.DataFrame()
for condition in conditions:
    filtered_data = merged_df[condition]
    selected_data = selected_data.append(filtered_data, ignore_index=True)

# Calculate revenue
selected_data['revenue'] = selected_data['L_EXTENDEDPRICE'] * (1 - selected_data['L_DISCOUNT'])

# Group by the necessary columns and sum the revenue
grouped_data = selected_data.groupby(['P_BRAND', 'P_CONTAINER', 'L_QUANTITY', 'P_SIZE', 'L_SHIPMODE', 'L_SHIPINSTRUCT'], as_index=False)['revenue'].sum()

# Write the final results to a CSV file
grouped_data.to_csv('query_output.csv', index=False)
