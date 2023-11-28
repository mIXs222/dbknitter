import pymongo
import pandas as pd
from bson import SON
from direct_redis import DirectRedis

# Functions to handle data fetching and querying
def get_mongo_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    part_collection = db['part']

    query = {
        "$or": [
            {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
            {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
            {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}}
        ]
    }
    
    parts_projection = {
        "_id": 0,
        "P_PARTKEY": 1,
        "P_SIZE": 1
    }

    parts = list(part_collection.find(query, parts_projection))
    return pd.DataFrame(parts)

def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    lineitem_df = r.get('lineitem')
    return lineitem_df

# Fetch data from MongoDB and Redis
parts_df = get_mongo_data()
lineitem_df = get_redis_data()

# Perform the analysis
result = lineitem_df.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply the specified conditions
conditions = [
    (result['P_BRAND'] == 'Brand#12') &
    (result['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (result['L_QUANTITY'] >= 1) & (result['L_QUANTITY'] <= 11) &
    (result['P_SIZE'] >= 1) & (result['P_SIZE'] <= 5) &
    (result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    (result['P_BRAND'] == 'Brand#23') &
    (result['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (result['L_QUANTITY'] >= 10) & (result['L_QUANTITY'] <= 20) &
    (result['P_SIZE'] >= 1) & (result['P_SIZE'] <= 10) &
    (result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    (result['P_BRAND'] == 'Brand#34') &
    (result['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (result['L_QUANTITY'] >= 20) & (result['L_QUANTITY'] <= 30) &
    (result['P_SIZE'] >= 1) & (result['P_SIZE'] <= 15) &
    (result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
]

result['Revenue'] = result.loc[:, 'L_EXTENDEDPRICE'] * (1 - result.loc[:, 'L_DISCOUNT'])
result['Selection'] = conditions[0] | conditions[1] | conditions[2]

# Calculate the total revenue
total_revenue = result[result['Selection']].groupby('L_PARTKEY')['Revenue'].sum().reset_index()

# Output results to CSV
total_revenue.to_csv('query_output.csv', index=False)
