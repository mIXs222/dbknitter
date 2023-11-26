from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']
# Get collection
lineitem_collection = mongo_db['lineitem']

# Redis connection
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis into a DataFrame
part_data = redis_connection.get('part')
if part_data is not None:
    part_df = pd.read_json(part_data)
else:
    raise Exception("Unable to retrieve 'part' data from Redis.")

# Convert MongoDB Cursor to DataFrame
lineitem_cursor = lineitem_collection.find()
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Use the query logic to combine data
combined_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply conditions as per SQL query
conditions = (
    (
        (combined_df['P_BRAND'] == 'Brand#12') &
        combined_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) &
        combined_df['L_QUANTITY'].between(1, 1 + 10) &
        combined_df['P_SIZE'].between(1, 5) &
        combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) &
        (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) | (
        (combined_df['P_BRAND'] == 'Brand#23') &
        combined_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']) &
        combined_df['L_QUANTITY'].between(10, 10 + 10) &
        combined_df['P_SIZE'].between(1, 10) &
        combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) &
        (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) | (
        (combined_df['P_BRAND'] == 'Brand#34') &
        combined_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']) &
        combined_df['L_QUANTITY'].between(20, 20 + 10) &
        combined_df['P_SIZE'].between(1, 15) &
        combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) &
        (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    )
)

# Apply the above filters to the DataFrame
filtered_df = combined_df[conditions]

# Calculate the REVENUE
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
result = filtered_df.groupby(level=0).agg({'REVENUE': 'sum'})

# Write result to a CSV file
result.to_csv('query_output.csv')
