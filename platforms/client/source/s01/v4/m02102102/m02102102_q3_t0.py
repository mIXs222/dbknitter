import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch orders from MySQL
mysql_cursor.execute("""
SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
FROM orders
WHERE O_ORDERDATE < '1995-03-15';
""")
orders_result = mysql_cursor.fetchall()
orders_columns = ['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']
df_orders = pd.DataFrame(orders_result, columns=orders_columns)

# Fetch customers from MongoDB
customers_result = mongo_customers.find({'C_MKTSEGMENT': 'BUILDING'}, {'_id': 0, 'C_CUSTKEY': 1})
df_customers = pd.DataFrame(list(customers_result))

# Fetch and decode lineitems from Redis
lineitem_keys = redis_conn.keys('lineitem:*')
lineitems = []
for key in lineitem_keys:
    lineitem = redis_conn.get(key.decode("utf-8"))
    if lineitem:
        lineitems.append(eval(lineitem.decode("utf-8")))

# Create DataFrame for lineitems from Redis
df_lineitems = pd.DataFrame(lineitems)

# Merge dataframes
merged_df = df_customers.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the WHERE conditions
filtered_df = merged_df[
    (merged_df['L_SHIPDATE'] > datetime(1995, 3, 15))
]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Perform grouping
grouped_df = filtered_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Save to CSV
sorted_df.to_csv('query_output.csv', index=False)
