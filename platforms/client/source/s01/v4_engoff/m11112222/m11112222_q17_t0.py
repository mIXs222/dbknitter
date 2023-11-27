from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)
lineitem_df = redis_client.get('lineitem')

# Check if lineitem data is a DataFrame, if not decode and parse it
if not isinstance(lineitem_df, pd.DataFrame):
    lineitem_df = pd.read_json(lineitem_df.decode('utf-8'), orient='records')

# Query MongoDB for parts of brand 23 and with MED BAG
part_criteria = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
part_df = pd.DataFrame(list(part_collection.find(part_criteria, {'P_PARTKEY': 1})))

# Merge parts with lineitem on P_PARTKEY and L_PARTKEY
merged_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate the average quantity and consider parts with quantity < 20% of this average
average_quantity = merged_df['L_QUANTITY'].mean()
small_quantity_threshold = average_quantity * 0.2
small_quantity_df = merged_df[merged_df['L_QUANTITY'] < small_quantity_threshold]

# Calculate the yearly lost revenue
small_quantity_df['L_LOST_REVENUE'] = small_quantity_df['L_EXTENDEDPRICE']
average_yearly_loss_revenue = small_quantity_df['L_LOST_REVENUE'].sum() / 7

# Save the result to a CSV file
output_df = pd.DataFrame([{'AverageYearlyLostRevenue': average_yearly_loss_revenue}])
output_df.to_csv('query_output.csv', index=False)
