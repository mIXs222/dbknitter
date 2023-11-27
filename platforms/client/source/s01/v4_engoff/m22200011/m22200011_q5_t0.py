# local_supplier_volume_query.py

import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Connect to Redis using direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the nations in the ASIA region from Redis
asia_nation_keys = []
regions_data = redis_client.get('region')
nations_data = redis_client.get('nation')
regions_df = pd.read_json(regions_data)
nations_df = pd.read_json(nations_data)
asia_region_key = regions_df[regions_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].values[0]
asia_nations_df = nations_df[nations_df['N_REGIONKEY'] == asia_region_key]
asia_nation_keys = asia_nations_df['N_NATIONKEY'].tolist()

# Execute query in MySQL to get suppliers and customers in ASIA
with mysql_connection.cursor() as cursor:
    # Suppliers in ASIA
    sql = "SELECT * FROM supplier WHERE S_NATIONKEY IN (%s)" % ','.join(map(str, asia_nation_keys))
    cursor.execute(sql)
    suppliers = cursor.fetchall()
    suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    # Customers in ASIA
    sql = "SELECT * FROM customer WHERE C_NATIONKEY IN (%s)" % ','.join(map(str, asia_nation_keys))
    cursor.execute(sql)
    customers = cursor.fetchall()
    customers_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

mysql_connection.close()

# Get orders and lineitems from MongoDB
orders_collection = mongodb['orders']
lineitem_collection = mongodb['lineitem']

# Order by date range
start_date = datetime(1990, 1, 1)
end_date = datetime(1995, 1, 1)
query = {'O_ORDERDATE': {'$gte': start_date, '$lt': end_date}}
orders_df = pd.DataFrame(list(orders_collection.find(query)))

# Get all lineitems
lineitems_df = pd.DataFrame(list(lineitem_collection.find()))

# Merge and calculate revenues
merge_df = pd.merge(
    lineitems_df,
    orders_df,
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY',
    how='inner'
)
merge_df = pd.merge(
    merge_df,
    customers_df,
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY',
    how='inner'
)
merge_df = pd.merge(
    merge_df,
    suppliers_df,
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY',
    how='inner'
)
merge_df['REVENUE'] = merge_df['L_EXTENDEDPRICE'] * (1 - merge_df['L_DISCOUNT'])

# Filter customers and suppliers from ASIA
merge_df = merge_df[merge_df['C_NATIONKEY'].isin(asia_nation_keys) & merge_df['S_NATIONKEY'].isin(asia_nation_keys)]

# Group by nation and calculate revenue
result = merge_df.groupby('C_NATIONKEY')['REVENUE'].sum().reset_index()
result = pd.merge(result, nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='left')
result = result[['N_NAME', 'REVENUE']]
result = result.sort_values(by='REVENUE', ascending=False)

# Write result to CSV
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
