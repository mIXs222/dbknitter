import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_nation = mongo_db['nation']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve nation data from MongoDB
nation_data = list(mongo_nation.find({}, {'_id': 0}))
nation_df = pd.DataFrame(nation_data)
india_japan_nations = nation_df[nation_df['N_NAME'].isin(['INDIA', 'JAPAN'])]

# Retrieve supplier and customer data from MySQL
supplier_query = "SELECT * FROM supplier WHERE S_NATIONKEY in (%s, %s)"
customer_query = "SELECT * FROM customer WHERE C_NATIONKEY in (%s, %s)"

india_nkey = india_japan_nations[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]
japan_nkey = india_japan_nations[nation_df['N_NAME'] == 'JAPAN']['N_NATIONKEY'].iloc[0]

mysql_cursor.execute(supplier_query, (india_nkey, japan_nkey))
supplier_data = mysql_cursor.fetchall()

mysql_cursor.execute(customer_query, (india_nkey, japan_nkey))
customer_data = mysql_cursor.fetchall()

supplier_df = pd.DataFrame(supplier_data, columns=[desc[0] for desc in mysql_cursor.description])
customer_df = pd.DataFrame(customer_data, columns=[desc[0] for desc in mysql_cursor.description])

# Retrieve orders and lineitem data from Redis
orders_df = pd.read_json(redis_conn.get('orders').decode('utf-8'))
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'))

# Filtering for 1995 and 1996 orders
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'].dt.year == 1995) | (orders_df['O_ORDERDATE'].dt.year == 1996)]

# Compute the required relationships and calculate revenue
shipments_df = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
shipments_df = pd.merge(shipments_df, customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
shipments_df = pd.merge(shipments_df, supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
shipments_df['revenue'] = shipments_df['L_EXTENDEDPRICE'] * (1 - shipments_df['L_DISCOUNT'])

# Apply the conditions described in the Volume Shipping Query
filtered_shipments = shipments_df[
    ((shipments_df['S_NATIONKEY'] == india_nkey) & (shipments_df['C_NATIONKEY'] == japan_nkey)) |
    ((shipments_df['S_NATIONKEY'] == japan_nkey) & (shipments_df['C_NATIONKEY'] == india_nkey))
]

grouped_shipments = filtered_shipments.groupby(
    ['S_NATIONKEY', 'C_NATIONKEY', orders_df['O_ORDERDATE'].dt.year]
).agg(
    {'revenue': 'sum'}
).reset_index()

# Map nation keys to nation names
grouped_shipments['supplier_nation'] = grouped_shipments['S_NATIONKEY'].map(
    india_japan_nations.set_index('N_NATIONKEY')['N_NAME']
)
grouped_shipments['customer_nation'] = grouped_shipments['C_NATIONKEY'].map(
    india_japan_nations.set_index('N_NATIONKEY')['N_NAME']
)

# Rename columns appropriately and sort by the specified fields
final_result = grouped_shipments.rename(
    columns={'O_ORDERDATE': 'year', 'revenue': 'gross_revenue'}
).sort_values(by=['supplier_nation', 'customer_nation', 'year'])

# Write the final result to a CSV file
final_result.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
