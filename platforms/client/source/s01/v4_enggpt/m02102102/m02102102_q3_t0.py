import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query the 'orders' table in MySQL
with mysql_conn.cursor() as cursor:
    orders_query = """
        SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY
        FROM orders
        WHERE O_ORDERDATE < '1995-03-15';
    """
    cursor.execute(orders_query)
    orders_data = cursor.fetchall()
    df_orders = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

# Query the 'customer' table in MongoDB
customer_query = {'C_MKTSEGMENT': 'BUILDING'}
customers_data = list(mongodb.customer.find(customer_query, {'_id': 0, 'C_CUSTKEY': 1}))
df_customers = pd.DataFrame(customers_data)

# Get 'lineitem' table from Redis
lineitem_data = redis_client.get('lineitem')
df_lineitem = pd.read_json(lineitem_data)
df_lineitem = df_lineitem[df_lineitem['L_SHIPDATE'] > '1995-03-15']

# Data merging
df_merged = pd.merge(df_customers, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merged = pd.merge(df_merged, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_merged['REVENUE'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Group by requirements
df_grouped = df_merged.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()

# Sorting the results
df_sorted = df_grouped.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Save results to CSV
df_sorted.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
