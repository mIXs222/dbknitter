import pandas as pd
import pymysql
import pymongo
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis
customer_df = pd.read_json(redis_client.get('customer').decode('utf-8'))

# Get region data with R_NAME = 'ASIA' from MongoDB
region_df = pd.DataFrame(list(mongodb_db['region'].find({'R_NAME': 'ASIA'})))

# Get lineitem data from MongoDB
lineitem_df = pd.DataFrame(list(mongodb_db['lineitem'].find()))

# Get nation data from MySQL
nation_query = "SELECT * FROM nation"
mysql_cursor.execute(nation_query)
nation_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[desc[0] for desc in mysql_cursor.description])

# Get supplier data from MySQL
supplier_query = "SELECT * FROM supplier"
mysql_cursor.execute(supplier_query)
supplier_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[desc[0] for desc in mysql_cursor.description])

# Get orders data from MySQL with date between 1990-01-01 and 1994-12-31
orders_query = "SELECT * FROM orders WHERE O_ORDERDATE BETWEEN '1990-01-01' AND '1994-12-31'"
mysql_cursor.execute(orders_query)
orders_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[desc[0] for desc in mysql_cursor.description])

# Close MySQL connection
mysql_conn.close()

# Join operation to combine data
filtered_orders = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
filtered_orders_lineitem = filtered_orders.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
filtered_nation_supplier = nation_df.merge(supplier_df, left_on='N_NATIONKEY', right_on='S_NATIONKEY')
filtered_result = filtered_nation_supplier.merge(filtered_orders_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
final_result = filtered_result.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Calculate the revenue
final_result['REVENUE'] = final_result['L_EXTENDEDPRICE'] * (1 - final_result['L_DISCOUNT'])

# Group by nation name and sort by revenue descending
output = final_result.groupby('N_NAME', as_index=False)['REVENUE'].sum()
output = output.sort_values(by='REVENUE', ascending=False)

# Write result to CSV file
output.to_csv('query_output.csv', index=False)
