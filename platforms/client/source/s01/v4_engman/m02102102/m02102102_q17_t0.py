import pymongo
import direct_redis
import pandas as pd

# Connect to mongodb
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = client["tpch"]

# Load 'part' collection
parts_df = pd.DataFrame(list(mongodb_db.part.find({"P_BRAND": "BRAND#23", "P_CONTAINER": "MED BAG"})))

# Connect to redis
redis_db = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load 'lineitem' as Pandas DataFrame
lineitem_df = redis_db.get('lineitem')

# Calculate the average quantity of small-quantity orders
small_qty_average = lineitem_df.loc[
    (lineitem_df['L_PARTKEY'].isin(parts_df['P_PARTKEY'])) &
    (lineitem_df['L_QUANTITY'] < lineitem_df['L_QUANTITY'].mean() * 0.2)
]['L_QUANTITY'].mean()

# Calculate the undiscouted gross loss
gross_loss = small_qty_average * parts_df['P_RETAILPRICE'].mean()

# There is no time range provided, so assuming 7 years as stated in the query
average_yearly_loss = gross_loss / 7

# Output the result
result = pd.DataFrame([{'average_yearly_loss': average_yearly_loss}])
result.to_csv('query_output.csv', index=False)
