from pymongo import MongoClient
from direct_redis import DirectRedis
from datetime import datetime
import pandas as pd

# Function to convert MongoDB cursor to DataFrame
def mongo_cursor_to_dataframe(cursor):
    return pd.DataFrame(list(cursor))

# Function to save DataFrame to CSV
def dataframe_to_csv(df, filename):
    df.to_csv(filename, index=False)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Retrieve lineitem data from MongoDB within the specified date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
lineitems = mongo_cursor_to_dataframe(lineitem_collection.find({
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
}, projection={'_id': False}))

# Compute revenue
lineitems['TOTAL_REVENUE'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])
revenue0 = lineitems.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()
revenue0.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'}, inplace=True)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve supplier data from Redis and convert to DataFrame
supplier_json = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_json, orient='records')

# Merge the dataframes to match the SQL query's JOIN and WHERE clauses
result_df = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the supplier(s) with the maximum TOTAL_REVENUE
max_total_revenue = result_df['TOTAL_REVENUE'].max()
max_revenue_suppliers = result_df[result_df['TOTAL_REVENUE'] == max_total_revenue]

# Select and order the final columns
final_result = max_revenue_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the result to CSV
dataframe_to_csv(final_result, 'query_output.csv')
