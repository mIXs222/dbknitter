# query_exec.py
import pandas as pd
from pymongo import MongoClient
import redis
from datetime import datetime
import csv

# Create a MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Get lineitem data from MongoDB
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)
lineitem_query = {
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
}
lineitem_data = pd.DataFrame(list(lineitem_collection.find(lineitem_query, projection={'_id': False})))

# Create a Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Get part data from Redis
part_keys = redis_client.keys('part:*')
part_data = pd.DataFrame([eval(redis_client.get(key)) for key in part_keys])

# Filtering line items with the corresponding parts
lineitem_data['L_PARTKEY'] = lineitem_data['L_PARTKEY'].astype(int) # Ensure correct data type
combined_data = lineitem_data.merge(part_data, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Promotional & Total Revenue Calculation
combined_data['ADJUSTED_PRICE'] = combined_data['L_EXTENDEDPRICE'] * (1 - combined_data['L_DISCOUNT'])
promo_revenue = combined_data[combined_data['P_TYPE'].str.startswith('PROMO')]['ADJUSTED_PRICE'].sum()
total_revenue = combined_data['ADJUSTED_PRICE'].sum()
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Writing output to a file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional Revenue', 'Total Revenue', 'Promotional Revenue Percentage'])
    writer.writerow([promo_revenue, total_revenue, promo_percentage])

print('The query has been executed and the results are saved in "query_output.csv".')
