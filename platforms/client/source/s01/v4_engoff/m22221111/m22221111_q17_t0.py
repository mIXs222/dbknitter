import pymongo
import pandas as pd
import redis
from direct_redis import DirectRedis

# MongoDB connection
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']
lineitem_collection = mongodb['lineitem']

# Redis connection
r = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
part_df_json = r.get('part')
part_df = pd.read_json(part_df_json)

# Filter parts with brand 23 and container type 'MED BAG'
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Get lineitem data from MongoDB
lineitems = list(lineitem_collection.find(
    {
        'L_PARTKEY': {'$in': filtered_parts['P_PARTKEY'].tolist()}
    },
    {
        '_id': 0,
        'L_ORDERKEY': 1, 'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_QUANTITY': 1
    }
))
lineitem_df = pd.DataFrame(lineitems)

# Calculate the average quantity ordered for these parts across all orders
avg_quantity = lineitem_df['L_QUANTITY'].mean()

# Calculate the average yearly revenue that would be lost
threshold_quantity = avg_quantity * 0.2
lost_revenue_df = lineitem_df[lineitem_df['L_QUANTITY'] < threshold_quantity]

# Group by year (extract year from L_SHIPDATE)
lost_revenue_df['L_SHIPDATE'] = pd.to_datetime(lost_revenue_df['L_SHIPDATE'])
lost_revenue_df['YEAR'] = lost_revenue_df['L_SHIPDATE'].dt.year

# Calculate the average yearly gross loss
average_yearly_loss = lost_revenue_df.groupby('YEAR')['L_EXTENDEDPRICE'].sum().mean()

# Write the output to csv file
output_df = pd.DataFrame({'Average_Yearly_Loss_Revenue': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
