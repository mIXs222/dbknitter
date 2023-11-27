# query.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitem_collection = mongodb['lineitem']
lineitem_data = pd.DataFrame(list(lineitem_collection.find(
    {'L_SHIPMODE': {'$exists': True}},
    {'_id': 0, 'L_PARTKEY': 1, 'L_QUANTITY': 1, 'L_EXTENDEDPRICE': 1}
)))

# Connect to Redis
redis = DirectRedis(port=6379, host='redis')
part_data = pd.read_json(redis.get('part'), orient='records')

# Filter parts with the specified brand and type
target_parts = part_data[(part_data['P_BRAND'] == 'Brand#23') & (part_data['P_CONTAINER'] == 'MED BAG')]

# Filter lineitems with the specified parts
target_lineitems = lineitem_data[lineitem_data['L_PARTKEY'].isin(target_parts['P_PARTKEY'])]

# Calculate the average quantity
average_quantity = target_lineitems['L_QUANTITY'].mean()

# Calculate the threshold (20% of the average quantity)
threshold_quantity = 0.2 * average_quantity

# Filter orders with less than the threshold quantity
small_quantity_orders = target_lineitems[target_lineitems['L_QUANTITY'] < threshold_quantity]

# Calculate the total loss in revenue
total_loss_revenue = small_quantity_orders['L_EXTENDEDPRICE'].sum()

# Calculate the average yearly loss in revenue (assuming a 7-year span)
average_yearly_loss_revenue = total_loss_revenue / 7

# Save to CSV
result = pd.DataFrame({'Average_Yearly_Loss_Revenue': [average_yearly_loss_revenue]})
result.to_csv('query_output.csv', index=False)
