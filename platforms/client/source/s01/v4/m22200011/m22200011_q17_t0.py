import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Function to get DataFrame from MongoDB
def get_mongo_df(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    cursor = collection.find({}, {'_id': 0})  # Exclude the _id field
    df = pd.DataFrame(list(cursor))
    return df

# Function to get DataFrame from Redis
def get_redis_df(redis_conn, tablename):
    df_json = redis_conn.get(tablename)
    df = pd.read_json(df_json, orient='records')
    return df

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_df = get_mongo_df(mongo_client, 'tpch', 'lineitem')

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
redis_df = get_redis_df(redis_conn, 'part')

# Perform the SQL-like query using pandas operations

# Filtering part table for P_BRAND and P_CONTAINER
filtered_parts = redis_df[(redis_df['P_BRAND'] == 'Brand#23') &
                          (redis_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the AVG(L_QUANTITY) for each part
avg_qty_by_part = mongo_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_by_part['AVG_QTY'] = avg_qty_by_part['L_QUANTITY'] * 0.2
avg_qty_by_part.drop('L_QUANTITY', axis=1, inplace=True)

# Merge filtered_parts with lineitem on P_PARTKEY
merged_df = pd.merge(filtered_parts, mongo_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter on L_QUANTITY < 0.2 * AVG(L_QUANTITY)
merged_df = pd.merge(merged_df, avg_qty_by_part, on='L_PARTKEY')
final_df = merged_df[merged_df['L_QUANTITY'] < merged_df['AVG_QTY']]

# Calculate the final result
result = final_df['L_EXTENDEDPRICE'].sum() / 7.0

# Create a DataFrame to store the result
result_df = pd.DataFrame({'AVG_YEARLY': [result]})

# Write the result to query_output.csv
result_df.to_csv('query_output.csv', index=False)
