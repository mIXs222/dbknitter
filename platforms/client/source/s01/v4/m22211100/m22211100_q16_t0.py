import pymongo
import redis
import pandas as pd

# Connecting to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_col = mongo_db["supplier"]
partsupp_col = mongo_db["partsupp"]

# Fetch supplier data excluding those with 'Customer Complaints' in comments
supplier_data = pd.DataFrame(list(supplier_col.find(
    {"S_COMMENT": {"$not": {"$regex": "Customer.*Complaints"}}},
    {"_id": 0, "S_SUPPKEY": 1}
)))

# Fetch partsupp data
partsupp_data = pd.DataFrame(list(partsupp_col.find({}, {"_id": 0})))

# Combine partsupp with supplier (and filter suppliers)
partsupp_supplier_data = partsupp_data.merge(supplier_data, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")

# Connecting to Redis
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Fetch part data from Redis
part_keys = r.keys('part:*')
part_data_list = [eval(r.get(key)) for key in part_keys]
part_data = pd.DataFrame(part_data_list)

# Perform the SQL-like operations with Pandas
part_query_result = part_data[
    (part_data['P_BRAND'] != 'Brand#45') &
    (~part_data['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_data['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Join partsupp_supplier_data with part_query_result
result = partsupp_supplier_data.merge(
    part_query_result,
    left_on="PS_PARTKEY", 
    right_on="P_PARTKEY"
)

# Group, count distinct supplier, and sort
output = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
               .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')) \
               .reset_index() \
               .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV
output.to_csv('query_output.csv', index=False)
