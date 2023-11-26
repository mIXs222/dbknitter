# query.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Query parts from MongoDB
query_brand12 = {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]},
                 "P_SIZE": {"$gte": 1, "$lte": 5}}
query_brand23 = {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]},
                 "P_SIZE": {"$gte": 1, "$lte": 10}}
query_brand34 = {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]},
                 "P_SIZE": {"$gte": 1, "$lte": 15}}
part_data = pd.DataFrame(list(part_collection.find({"$or": [query_brand12, query_brand23, query_brand34]})))

# Redis connection and query
redis_client = direct_redis.DirectRedis(host="redis", port=6379)
df_lineitem_str = redis_client.get('lineitem')
df_lineitem = pd.read_json(df_lineitem_str)

# Filter lineitem data
filter_conditions = (
    ((df_lineitem['L_QUANTITY'] >= 1) & (df_lineitem['L_QUANTITY'] <= 11) &
     (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((df_lineitem['L_QUANTITY'] >= 10) & (df_lineitem['L_QUANTITY'] <= 20) &
     (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((df_lineitem['L_QUANTITY'] >= 20) & (df_lineitem['L_QUANTITY'] <= 30) &
     (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
)
df_lineitem_filtered = df_lineitem[filter_conditions]

# Combine and calculate revenue
result = pd.merge(df_lineitem_filtered, part_data, left_on="L_PARTKEY", right_on="P_PARTKEY")
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Sum the revenue
revenue_sum = result[['REVENUE']].sum()

# Write to CSV file
revenue_sum.to_csv('query_output.csv', index=False)
