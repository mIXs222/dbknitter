# save as query_execute.py

import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Fetch lineitem data with filter on SHIPDATE
query = {
    'L_SHIPDATE': {
        '$gte': datetime.strptime('1995-09-01', '%Y-%m-%d'),
        '$lt': datetime.strptime('1995-10-01', '%Y-%m-%d')
    }
}
lineitem_columns = ['L_PARTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT']
lineitem_data = pd.DataFrame(list(lineitem_collection.find(query, {column: 1 for column in lineitem_columns})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_data_raw = redis_client.get('part')
part_data = pd.read_json(part_data_raw.decode('utf-8'))

# Merge lineitem and part dataframes on PARTKEY
merged_data = pd.merge(
    lineitem_data,
    part_data,
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Calculate PROMO_REVENUE
merged_data['ADJUSTED_PRICE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])
merged_data['PROMO_REVENUE'] = merged_data.apply(lambda row: row['ADJUSTED_PRICE'] if row['P_TYPE'].startswith('PROMO') else 0, axis=1)
promo_revenue = 100.0 * merged_data['PROMO_REVENUE'].sum() / merged_data['ADJUSTED_PRICE'].sum()

# Write to CSV
output_df = pd.DataFrame({'PROMO_REVENUE': [promo_revenue]})
output_df.to_csv('query_output.csv', index=False)
