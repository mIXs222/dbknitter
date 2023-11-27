import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
lineitem_collection = mongodb["lineitem"]

# Get the data from lineitem where P_BRAND is 'Brand#23' and P_CONTAINER is 'MED BAG'
lineitem_df = pd.DataFrame(list(lineitem_collection.find({"L_SHIPINSTRUCT": "DELIVER IN PERSON"})))
brand_23_med_bag_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].str.contains('TRUCK')) &
    (lineitem_df['L_COMMENT'].str.contains('special')) &
    (lineitem_df['L_COMMENT'].str.contains('packages'))
]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_data = redis_client.get('part')
part_df = pd.DataFrame(part_data)

# Filter the part table
filtered_parts_df = part_df[
    (part_df['P_BRAND'] == 'Brand#23') &
    (part_df['P_CONTAINER'] == 'MED BAG')
]

# Calculate the average quantity for the filtered parts
average_quantity = filtered_parts_df['P_QUANTITY'].mean()

# Calculate the average yearly gross loss in revenue
brand_23_med_bag_df['Potential_Loss'] = brand_23_med_bag_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] if row['L_QUANTITY'] < (0.2 * average_quantity) else 0, axis=1)
average_yearly_loss = brand_23_med_bag_df['Potential_Loss'].sum() / 7  # Assume 7 years

# Store the result
result = pd.DataFrame([{'Average_Yearly_Loss': average_yearly_loss}])
result.to_csv('query_output.csv', index=False)
