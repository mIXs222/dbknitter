import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]

# Get the lineitem table from MongoDB
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find(
    {"L_RETURNFLAG": {"$exists": True}},
    {
        "L_PARTKEY": 1,
        "L_QUANTITY": 1,
        "L_EXTENDEDPRICE": 1,
        "_id": 0
    }
)))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Get the part table from Redis
part_str = redis_client.get('part')
part_df = pd.read_json(part_str, orient='records')

# Data processing
# Filter parts with brand 'Brand#23' and container 'MED BAG'
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]
# Join to get relevant lineitems
filtered_lineitems = lineitem_df[lineitem_df['L_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]
# Calculate the average quantity
avg_quantity = filtered_lineitems['L_QUANTITY'].mean()
# Calculate the average yearly loss in revenue for small quantities
small_qty_revenue_loss = filtered_lineitems[filtered_lineitems['L_QUANTITY'] < (0.2 * avg_quantity)]['L_EXTENDEDPRICE'].sum() / 7

# Write to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Average Yearly Revenue Loss'])
    writer.writerow([small_qty_revenue_loss])
