import pandas as pd
import pymysql
import pymongo
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_cursor.execute("SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT FROM customer")
customers = mysql_cursor.fetchall()

# Structure the DataFrame for customers
df_customers = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])

# Fetch data from MongoDB
lineitem_collection = mongo_db['lineitem']
lineitem_query = {
    'L_SHIPDATE': {'$gte': datetime(1993, 10, 1), '$lt': datetime(1994, 1, 2)}
}
lineitems = list(lineitem_collection.find(lineitem_query))

# Structure the DataFrame for lineitems
df_lineitems = pd.DataFrame(lineitems)

# Calculate the revenue lost
df_lineitems['REVENUE_LOST'] = df_lineitems['L_EXTENDEDPRICE'] * (1 - df_lineitems['L_DISCOUNT'])

# Fetch data from Redis
nation_data = redis_client.get('nation').decode('utf-8')
orders_data = redis_client.get('orders').decode('utf-8')

# Structure the DataFrame for nation and orders
df_nation = pd.read_csv(pd.compat.StringIO(nation_data))
df_orders = pd.read_csv(pd.compat.StringIO(orders_data))

# Merge the dataframes to get results
merged_df = pd.merge(df_customers, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by customer information and calculate revenue lost
result = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'N_NAME'])['REVENUE_LOST'] \
    .sum().reset_index(name='REVENUE_LOST')

# Sort the results
result = result.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Output results to query_output.csv
result.to_csv('query_output.csv', index=False)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
