# query.py content

from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
part = pd.DataFrame(list(mongodb['part'].find()))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.DataFrame(eval(r.get('lineitem')))

# For each of the three types of parts, gather the relevant lineitems and calculate the revenue
def get_type_revenue(df, brand_id, containers, quantity_min, quantity_max, size_min, size_max):
    matching_parts = part[
        (part['P_BRAND'].eq(brand_id)) &
        (part['P_CONTAINER'].isin(containers)) &
        (part['P_SIZE'].between(size_min, size_max))
    ]
    matching_lineitems = df[
        (df['L_PARTKEY'].isin(matching_parts['P_PARTKEY'])) &
        (df['L_QUANTITY'].between(quantity_min, quantity_max)) &
        (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (df['L_SHIPINSTRUCT'].eq('DELIVER IN PERSON'))
    ]
    revenue = (matching_lineitems['L_EXTENDEDPRICE'] * (1 - matching_lineitems['L_DISCOUNT'])).sum()
    return revenue

# Typewise part info (brand_id, containers, quantity_min, quantity_max, size_min, size_max)
types_info = [
    (12, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], 1, 11, 1, 5), 
    (23, ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], 10, 20, 1, 10), 
    (34, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], 20, 30, 1, 15)
]

total_revenue = sum(get_type_revenue(lineitem_df, *info) for info in types_info)

# Writing total_revenue to a file
result_df = pd.DataFrame([{'REVENUE': total_revenue}])
result_df.to_csv('query_output.csv', index=False)
