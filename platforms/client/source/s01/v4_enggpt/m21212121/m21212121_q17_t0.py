import pymongo
import pandas as pd
from bson.regex import Regex
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]

# Query lineitem table
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find({
    "L_SHIPMODE": Regex(r'.*')  # Assuming we want to load all documents, this is a workaround for MongoDB
}, {
    "_id": 0, 
    "L_PARTKEY": 1, 
    "L_QUANTITY": 1,
    "L_EXTENDEDPRICE": 1
})))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query part table
part_df = pd.read_json(r.get('part'))

# Filtering for 'Brand#23' and 'MED BAG'
part_df = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Merging lineitem and part dataframe on part key
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate average quantity for each part
part_avg_quantity = merged_df.groupby('P_PARTKEY')['L_QUANTITY'].mean().reset_index()

# Calculate 20% of average quantity for each part
part_avg_quantity['qty_20_percent'] = part_avg_quantity['L_QUANTITY'] * 0.2

# Join with merged_df to filter further based on L_QUANTITY < 20% of avg quantity
final_df = pd.merge(merged_df, part_avg_quantity[['P_PARTKEY', 'qty_20_percent']], on='P_PARTKEY')
final_df = final_df[final_df['L_QUANTITY'] < final_df['qty_20_percent']]

# Calculate average yearly extended price
final_df['avg_yearly_ext_price'] = final_df['L_EXTENDEDPRICE'] / 7.0

# Output result
final_result = final_df.groupby('P_PARTKEY')['avg_yearly_ext_price'].mean().reset_index()
final_result.to_csv('query_output.csv', index=False)
