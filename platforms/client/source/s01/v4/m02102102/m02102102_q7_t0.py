import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_customer = mongo_db['customer']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load lineitem from Redis
lineitem_df = redis_client.get('lineitem')

# Execute MySQL query for supplier and nation
mysql_cursor.execute("""
SELECT S_SUPPKEY, S_NATIONKEY, N1.N_NAME AS SUPP_NATION
FROM supplier, nation AS N1
WHERE S_NATIONKEY = N1.N_NATIONKEY AND N1.N_NAME IN ('JAPAN', 'INDIA')
""")
suppliers_nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY', 'SUPP_NATION'])

# Execute MySQL query for orders
mysql_cursor.execute("""
SELECT O_ORDERKEY, O_CUSTKEY
FROM orders
""")
orders = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY'])
mysql_cursor.close()
mysql_conn.close()

# Fetch customer data from MongoDB
customer_data = list(mongo_customer.find({'C_NATIONKEY': {'$in': [suppliers_nations['S_NATIONKEY'].unique().tolist()]}}))
customers = pd.DataFrame(customer_data)

# Rename customer columns
customers.rename(columns={
    'C_CUSTKEY': 'C_CUSTKEY',
    'C_NATIONKEY': 'C_NATIONKEY',
    'C_NAME': 'C_NAME'
}, inplace=True)

# Join supplier and orders with lineitem DataFrame
lineitem_orders_suppliers = lineitem_df.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
lineitem_orders_suppliers_nations = lineitem_orders_suppliers.merge(suppliers_nations, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='inner')

# Join the result with customers DataFrame
final_df = lineitem_orders_suppliers_nations.merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
final_df['L_YEAR'] = pd.to_datetime(final_df['L_SHIPDATE']).dt.year

# Filter and calculate volume
final_df = final_df.query("SUPP_NATION in ['JAPAN', 'INDIA'] and C_NATIONKEY in ['JAPAN', 'INDIA'] and L_SHIPDATE >= '1995-01-01' and L_SHIPDATE <= '1996-12-31'")
final_df['VOLUME'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by SUPP_NATION, C_NATIONKEY, and L_YEAR
result = final_df.groupby(['SUPP_NATION', 'C_NATIONKEY', 'L_YEAR']).agg({'VOLUME': 'sum'}).reset_index()
result.rename(columns={'C_NATIONKEY': 'CUST_NATION'}, inplace=True)
result.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write the result to csv
result.to_csv('query_output.csv', index=False)
