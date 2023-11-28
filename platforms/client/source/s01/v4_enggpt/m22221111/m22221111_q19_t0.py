import pymongo
import pandas as pd
import direct_redis

# Function to create dataframe from Redis
def get_dataframe_from_redis(redis_conn, key):
    data = redis_conn.get(key)
    if data:
        df = pd.read_json(data)
    else:
        df = pd.DataFrame()
    return df

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
tpch_mongo_db = mongo_client["tpch"]
lineitem_collection = tpch_mongo_db["lineitem"]

# Extract lineitem table from MongoDB
lineitem_query = {
    "$or": [
        {
            "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON",
            "L_QUANTITY": {"$gte": 1, "$lte": 11},
        },
        {
            "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON",
            "L_QUANTITY": {"$gte": 10, "$lte": 20},
        },
        {
            "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON",
            "L_QUANTITY": {"$gte": 20, "$lte": 30},
        },
    ]
}
lineitem_cursor = lineitem_collection.find(lineitem_query)
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Extract part table from Redis
part_df = get_dataframe_from_redis(redis_conn, 'part')

# Combine the extracted data
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply the conditions for each brand
conditions = [
    (merged_df['P_BRAND'] == 'Brand#12') & (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) & (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 5),
    (merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) & (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 10),
    (merged_df['P_BRAND'] == 'Brand#34') & (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) & (merged_df['P_SIZE'] >= 1) & (merged_df['P_SIZE'] <= 15),
]

# Create a column for revenue
merged_df['revenue'] = merged_df.apply(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])), axis=1)

# Filter on the conditions and sum the revenue
filtered_df = merged_df[conditions[0] | conditions[1] | conditions[2]]
total_revenue = filtered_df['revenue'].sum()

# Write the result to a CSV file
output = pd.DataFrame({'Total Revenue': [total_revenue]})
output.to_csv('query_output.csv', index=False)
