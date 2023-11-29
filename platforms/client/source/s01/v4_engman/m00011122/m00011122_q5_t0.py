# query.py
import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# Connect to MySQL to get nation and region
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute(
        "SELECT N_NATIONKEY, N_NAME FROM nation "
        "JOIN region ON N_REGIONKEY=R_REGIONKEY "
        "WHERE R_NAME = 'ASIA'"
    )
    nations = cursor.fetchall()
mysql_conn.close()

nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

# Connect to MongoDB to get supplier and customer
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
supplier = mongo_db['supplier'].find({'S_NATIONKEY': {'$in': nation_df['N_NATIONKEY'].tolist()}})
customer = mongo_db['customer'].find({'C_NATIONKEY': {'$in': nation_df['N_NATIONKEY'].tolist()}})

supplier_df = pd.DataFrame(list(supplier))
customer_df = pd.DataFrame(list(customer))

# Connect to Redis to get orders and lineitem
redis = DirectRedis(host='redis', port=6379, db=0)
orders = pd.read_json(redis.get('orders'))
lineitem = pd.read_json(redis.get('lineitem'))

# Filter data by date
start_date = datetime(1990, 1, 1)
end_date = datetime(1995, 1, 1)
orders = orders[(orders['O_ORDERDATE'] >= start_date) & (orders['O_ORDERDATE'] < end_date)]

# Join orders with customers and suppliers
orders_customers = orders.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
orders_suppliers = lineitem.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter relevant orders containing both supplier and customer in Asia
relevant_orders = orders_customers.merge(orders_suppliers, on='O_ORDERKEY')

# Calculate the revenue
relevant_orders['REVENUE'] = relevant_orders['L_EXTENDEDPRICE'] * (1 - relevant_orders['L_DISCOUNT'])

# Group by nations and sum revenues
result = relevant_orders.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort the result
result = result.sort_values(by='REVENUE', ascending=False)

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
