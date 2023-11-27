# query_script.py
from pymongo import MongoClient
import pandas as pd
import direct_redis
import datetime

# Function to calculate promo revenue
def calculate_promo_revenue(lineitem_df, part_df):
    # Merge the dataframes on the part key
    merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Perform the calculation
    condition = merged_df['P_TYPE'].str.startswith('PROMO')
    promo_sum = ((merged_df[condition]['L_EXTENDEDPRICE'] * (1 - merged_df[condition]['L_DISCOUNT'])).sum())
    total_sum = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

    if total_sum == 0:
        return 0
    else:
        return (100.00 * promo_sum / total_sum)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']
part_df = pd.DataFrame(list(part_collection.find({}, {'_id': 0})))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filtering lineitem based on ship date
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 10, 1)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Calculate the promo revenue
promo_revenue = calculate_promo_revenue(filtered_lineitem_df, part_df)

# Write the result to a CSV file
result_df = pd.DataFrame([{'PROMO_REVENUE': promo_revenue}])
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mongo_client.close()
