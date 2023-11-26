from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB connection and getting 'lineitem' collection data
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_col = db.lineitem

# Extracting lineitem data
lineitem_pipeline = [
    {'$project': {
        'L_PARTKEY': 1,
        'L_QUANTITY': 1,
        'L_EXTENDEDPRICE': 1,
    }}
]
lineitem_data = list(lineitem_col.aggregate(lineitem_pipeline))
lineitem_df = pd.DataFrame(lineitem_data)

# Redis connection and getting 'part' data
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_data = redis_client.get('part')
part_df = pd.read_json(part_data)

# Subquery to calculate average quantity
avg_quantity = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean() * 0.2

# Main query execution
result = lineitem_df.join(part_df.set_index('P_PARTKEY'), on='L_PARTKEY')
result = result[(result['P_BRAND'] == 'Brand#23') &
                (result['P_CONTAINER'] == 'MED BAG') &
                (result['L_QUANTITY'] < result['L_PARTKEY'].map(avg_quantity))]
result = result['L_EXTENDEDPRICE'].sum() / 7.0

# Output the results to a CSV file
result_df = pd.DataFrame([{'AVG_YEARLY': result}])
result_df.to_csv('query_output.csv', index=False)
