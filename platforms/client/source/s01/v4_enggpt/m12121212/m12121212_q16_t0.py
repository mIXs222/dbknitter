# analysis.py
import pymongo
import direct_redis
import pandas as pd

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connection to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve parts data from MongoDB
parts_columns = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']
parts_data = list(mongo_db['part'].find(
    {
        'P_BRAND': {'$ne': 'Brand#45'},
        'P_TYPE': {'$not': {'$regex': r'^MEDIUM POLISHED'}},
        'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
    },
    projection={column: 1 for column in parts_columns}
))
df_parts = pd.DataFrame(parts_data)

# Retrieve partsupp data from MongoDB
partsupp_columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
partsupp_data = list(mongo_db['partsupp'].find(
    {},
    projection={column: 1 for column in partsupp_columns}
))
df_partsupp = pd.DataFrame(partsupp_data)

# Retrieve supplier data from Redis and create DataFrame
supplier_data = redis_client.get('supplier')
df_supplier = pd.read_json(supplier_data)
df_supplier = df_supplier[df_supplier['S_COMMENT'].apply(lambda x: 'Customer Complaints' not in x)]

# Merging dataframes
df_merged = pd.merge(df_partsupp, df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_merged = pd.merge(df_merged, df_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Grouping and counting suppliers
result = df_merged.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
                  .agg(SUPPLIER_CNT=pd.NamedAgg(column='S_SUPPKEY', aggfunc='nunique')) \
                  .reset_index()

# Sorting the results
result = result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output the results to a CSV file
result.to_csv('query_output.csv', index=False)
