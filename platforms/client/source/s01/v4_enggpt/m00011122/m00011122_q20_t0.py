import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    passwd='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve nation and part data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'CANADA'")
    nations = cursor.fetchall()
    nations_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

    cursor.execute("SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE 'forest%'")
    parts_df = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME'])

# Retrieve supplier data from MongoDB
supplier_data = mongodb.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_NATIONKEY': 1})
supplier_df = pd.DataFrame(list(supplier_data))

# Retrieve partsupp data from MongoDB
partsupp_data = mongodb.partsupp.find({}, {'_id': 0, 'PS_PARTKEY': 1, 'PS_SUPPKEY': 1})
partsupp_df = pd.DataFrame(list(partsupp_data))

# Retrieve lineitem data from Redis
lineitem_df = pd.read_msgpack(redis.get('lineitem'))

# Filter suppliers by Canada's nation key
canadian_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nations_df['N_NATIONKEY'].tolist())]

# Suppliers whose keys are in the set of supplier keys
canadian_suppliers_set = canadian_suppliers_df['S_SUPPKEY'].tolist()
filtered_partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(canadian_suppliers_set)]

# Part keys from 'partsupp' table where part names start with 'forest'
forest_parts_keys = parts_df['P_PARTKEY'].tolist()
filtered_partsupp_df = filtered_partsupp_df[filtered_partsupp_df['PS_PARTKEY'].isin(forest_parts_keys)]

# Calculate threshold quantities
lineitem_df['SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[(lineitem_df['SHIPDATE'] >= '1994-01-01') & (lineitem_df['SHIPDATE'] <= '1995-01-01')]

threshold_quantities = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() / 2
threshold_quantities_df = threshold_quantities.reset_index()
threshold_quantities_df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'THRESHOLD_QUANTITY']

# Merge to find the suppliers that meet conditions
result_df = filtered_partsupp_df.merge(threshold_quantities_df, on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Further filter for the final result to ensure quantities are greater than the threshold
result_df = result_df[result_df['PS_AVAILQTY'] > result_df['THRESHOLD_QUANTITY']]

# Final merge to get supplier names and addresses
final_result_df = result_df.merge(canadian_suppliers_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Select required columns and sort by supplier name
final_result_df = final_result_df[['S_NAME', 'S_ADDRESS']].sort_values(by=['S_NAME'])

# Save to CSV
final_result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
