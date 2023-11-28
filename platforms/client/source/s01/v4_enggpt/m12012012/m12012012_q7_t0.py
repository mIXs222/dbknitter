import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Get customer data from MySQL where the customers are from 'JAPAN' or 'INDIA'
query_customer = """
SELECT C_CUSTKEY, C_NATIONKEY 
FROM customer 
INNER JOIN nation ON C_NATIONKEY = N_NATIONKEY
WHERE N_NAME = 'JAPAN' OR N_NAME = 'INDIA';
"""
customers = pd.read_sql(query_customer, mysql_conn)
mysql_conn.close()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']

# Get supplier and nation data from MongoDB where the suppliers are from 'JAPAN' or 'INDIA'
suppliers = pd.DataFrame(list(mongodb['supplier'].find({'S_NATIONKEY': {'$in': list(mongodb['nation'].find({'N_NAME': {'$in': ['JAPAN', 'INDIA']}}, {'N_NATIONKEY': 1}))}})))

# Get orders data from MongoDB
orders = pd.DataFrame(list(mongodb['orders'].find()))

mongodb_client.close()

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis as DataFrame
lineitem_data = redis_client.get('lineitem')
lineitem = pd.read_json(lineitem_data)

# Merge dataframes to compute revenue and filter dates within 1995 and 1996
data_combined = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
data_combined = data_combined.merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
data_combined = data_combined.merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
data_combined['L_SHIPDATE'] = pd.to_datetime(data_combined['L_SHIPDATE'])
data_combined = data_combined[((data_combined['L_SHIPDATE'] >= '1995-01-01') & (data_combined['L_SHIPDATE'] <= '1996-12-31'))]

# Calculate revenue
data_combined['REVENUE'] = data_combined['L_EXTENDEDPRICE'] * (1 - data_combined['L_DISCOUNT'])

# Group by supplier nation, customer nation and year
grouped = data_combined.groupby([suppliers['S_NATIONKEY'], customers['C_NATIONKEY'], data_combined['L_SHIPDATE'].dt.year]).agg({'REVENUE': 'sum'}).reset_index()

# Get nation names
nation_mapping = {v: k for k, v in mongodb['nation'].find_one({}, {'N_NAME': 1, 'N_NATIONKEY': 1})}
grouped['SUPPLIER_NATION'] = grouped['S_NATIONKEY'].apply(lambda x: nation_mapping[x])
grouped['CUSTOMER_NATION'] = grouped['C_NATIONKEY'].apply(lambda x: nation_mapping[x])
grouped.drop(['S_NATIONKEY', 'C_NATIONKEY'], axis=1, inplace=True)

# Sort by supplier nation, customer nation, and year
grouped_sorted = grouped.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'L_SHIPDATE'])

# Export to csv
grouped_sorted.to_csv('query_output.csv', index=False)
