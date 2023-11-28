# revenue_analysis.py
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis Connection
redis_conn = DirectRedis(port=6379, host='redis')

# Retrieve data from redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Filter orders based on date
start_date = datetime(1990, 1, 1)
end_date = datetime(1994, 12, 31)
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Retrieve data from mysql
mysql_cursor.execute('SELECT * FROM customer')
customer_records = mysql_cursor.fetchall()
customer_df = pd.DataFrame(customer_records, columns=["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"])

# Retrieve data from mongodb
region_df = pd.DataFrame(list(mongo_db.region.find({}, {'_id': 0})))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({}, {'_id': 0})))

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()

# Join operations
region_asia_df = region_df[region_df['R_NAME'] == 'ASIA']
asia_nations_df = nation_df[nation_df['N_REGIONKEY'].isin(region_asia_df['R_REGIONKEY'])]
asia_customers_df = customer_df[customer_df['C_NATIONKEY'].isin(asia_nations_df['N_NATIONKEY'])]
asia_orders_df = orders_df[orders_df['O_CUSTKEY'].isin(asia_customers_df['C_CUSTKEY'])]
asia_lineitems_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(asia_orders_df['O_ORDERKEY'])]
asia_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations_df['N_NATIONKEY'])]

# Calculate the revenue
asia_lineitems_df['REVENUE'] = asia_lineitems_df['L_EXTENDEDPRICE'] * (1 - asia_lineitems_df['L_DISCOUNT'])
nation_revenue = asia_lineitems_df.groupby(asia_nations_df['N_NAME'].values)['REVENUE'].sum().reset_index()
nation_revenue.columns = ['N_NAME', 'TOTAL_REVENUE']
nation_revenue.sort_values('TOTAL_REVENUE', ascending=False, inplace=True)

# Write the final result to csv
nation_revenue.to_csv('query_output.csv', index=False)
