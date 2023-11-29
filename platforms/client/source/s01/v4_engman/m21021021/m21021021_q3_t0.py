import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve customer data from MySQL
mysql_cur.execute("SELECT C_CUSTKEY, C_MKTSEGMENT FROM customer WHERE C_MKTSEGMENT = 'BUILDING';")
customers = pd.DataFrame(mysql_cur.fetchall(), columns=['C_CUSTKEY', 'C_MKTSEGMENT'])

# Retrieve lineitem data from MongoDB
lineitems = pd.DataFrame(list(mongo_db.lineitem.find()))

# Retrieve orders data from Redis
orders_df = pd.read_json(redis.get('orders'))

# Close MySQL cursor and connection
mysql_cur.close()
mysql_conn.close()

# Processing the data
# Merge customers and orders on C_CUSTKEY
merged_df_customer_orders = pd.merge(customers, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Orders that had been ordered before 1995-03-05 but be shipped after 1995-03-15
filtered_orders = merged_df_customer_orders[
    (merged_df_customer_orders['O_ORDERDATE'] < '1995-03-05') &
    (merged_df_customer_orders['O_SHIPDATE'] > '1995-03-15')
]

# Merge filtered_orders with lineitems on O_ORDERKEY
final_df = pd.merge(filtered_orders, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate REVENUE
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by O_ORDERKEY and sum REVENUE, then sort by REVENUE in descending order
result = final_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()
result = result.sort_values(by='REVENUE', ascending=False)

# Write the result to query_output.csv
result.to_csv('query_output.csv', index=False, columns=['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
