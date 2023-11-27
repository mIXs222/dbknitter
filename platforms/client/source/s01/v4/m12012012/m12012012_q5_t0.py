import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT C_CUSTKEY, C_NATIONKEY
FROM customer
"""
customer_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and queries
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetching data from MongoDB tables into Pandas DataFrames
nation_df = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
orders_df = pd.DataFrame(list(mongo_db.orders.find({
    'O_ORDERDATE': {
        '$gte': datetime(1990, 1, 1),
        '$lt': datetime(1995, 1, 1)
    }
}, {'_id': 0})))

mongo_client.close()

# Redis connection and queries
redis_conn = DirectRedis(host='redis', port=6379, db=0)

region_df = pd.read_json(redis_conn.get('region'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

redis_conn.close()

# Joins and Filtering
# - Merge DataFrames in the same way as the SQL JOINs
# - Filter R_NAME by 'ASIA'
# - Compute REVENUE
# - Group by nation and sort by revenue

# First join nation to region where N_REGIONKEY = R_REGIONKEY and R_NAME = 'ASIA'
nation_region_df = pd.merge(nation_df, region_df[region_df['R_NAME'] == 'ASIA'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Next join nation_region_df to supplier on N_NATIONKEY = S_NATIONKEY
nation_supplier_df = pd.merge(nation_region_df, supplier_df, left_on='N_NATIONKEY', right_on='S_NATIONKEY')

# Join customer to nation_supplier_df on C_NATIONKEY = S_NATIONKEY
customer_nation_supplier_df = pd.merge(customer_df, nation_supplier_df, on='C_NATIONKEY')

# Join orders to customer_nation_supplier_df on C_CUSTKEY = O_CUSTKEY 
orders_customer_nation_supplier_df = pd.merge(orders_df, customer_nation_supplier_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Finally join lineitem to orders_customer_nation_supplier_df on L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY
final_df = pd.merge(lineitem_df, orders_customer_nation_supplier_df, left_on=['L_ORDERKEY', 'L_SUPPKEY'], right_on=['O_ORDERKEY', 'S_SUPPKEY'])

# Perform the aggregation to calculate REVENUE
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
result_df = final_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort the result by REVENUE in descending order
result_df.sort_values(by='REVENUE', ascending=False, inplace=True)

# Write the output to CSV
result_df.to_csv('query_output.csv', index=False)
