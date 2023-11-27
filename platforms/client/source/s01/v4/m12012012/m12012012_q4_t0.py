# Python code to execute the query across different data platforms
import pymongo
import pandas as pd
import direct_redis

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Query orders data from MongoDB
orders_pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {
                '$gte': '1993-07-01',
                '$lt': '1993-10-01'
            }
        }
    },
    {
        '$project': {
            'O_ORDERKEY': 1,
            'O_ORDERPRIORITY': 1
        }
    }
]
orders_data = list(orders_collection.aggregate(orders_pipeline))

# Connection to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Attempt to read the lineitem table from Redis as a DataFrame
try:
    lineitem_data = redis_client.get('lineitem')
    lineitem_df = pd.DataFrame(lineitem_data)
except Exception as e:
    print("An error occurred while fetching data from Redis:", str(e))
    lineitem_df = pd.DataFrame()

# Process data if lineitem data is fetched successfully
if not lineitem_df.empty:
    lineitem_df = lineitem_df.astype({'L_ORDERKEY': 'int64'})  # Making sure keys have the same type for merging
    # Filter lineitem data as per conditions
    lineitem_df_filtered = lineitem_df[
        lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']
    ]

    # Convert MongoDB data to DataFrame for processing
    orders_df = pd.DataFrame(orders_data)
    orders_df = orders_df.astype({'O_ORDERKEY': 'int64'})  # Match datatype for consistency during merge

    # Merge and filter data as per SQL query conditions
    merged_data = orders_df.merge(lineitem_df_filtered, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Perform grouping and counting
    result = merged_data.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

    # Save the data to CSV file
    result.to_csv('query_output.csv', index=False)
