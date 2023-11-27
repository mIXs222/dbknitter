import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
lineitem_query = """
SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE, L_SUPPKEY, L_PARTKEY
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';
"""
mysql_cursor.execute(lineitem_query)
lineitem_data = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE', 'L_SUPPKEY', 'L_PARTKEY'])
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']
customer_df = pd.DataFrame(list(customer_collection.find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NATIONKEY': 1})))

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.DataFrame(eval(redis_conn.get('nation')), columns=['N_NATIONKEY', 'N_NAME'])
supplier_df = pd.DataFrame(eval(redis_conn.get('supplier')), columns=['S_SUPPKEY','S_NATIONKEY'])
orders_df = pd.DataFrame(eval(redis_conn.get('orders')), columns=['O_ORDERKEY', 'O_CUSTKEY'])

# Join and filter the data
# Filter nations to India and Japan
nation_df = nation_df[(nation_df['N_NAME'] == 'INDIA') | (nation_df['N_NAME'] == 'JAPAN')]

# Map nation keys to customer and supplier dataframes
customer_df = customer_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
supplier_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Join lineitem with supplier and order with customer
lineitem_supplier_df = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
order_customer_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Final join and calculate gross discounted revenue
result_df = lineitem_supplier_df.merge(order_customer_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result_df = result_df[result_df['N_NAME_x'] != result_df['N_NAME_y']]  # Suppliers and customers should be from different nations

# Calculate gross discounted revenue
result_df['revenue'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Select columns
result_df['year'] = pd.to_datetime(result_df['L_SHIPDATE']).dt.year
output_df = result_df[['N_NAME_x', 'N_NAME_y', 'year', 'revenue']]
output_df.columns = ['supplier_nation', 'customer_nation', 'year', 'revenue']

# Group by the required fields and sum revenues
output_df = output_df.groupby(['supplier_nation', 'customer_nation', 'year']).agg({'revenue': 'sum'}).reset_index()
output_df = output_df.sort_values(by=['supplier_nation', 'customer_nation', 'year'])

# Write to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
