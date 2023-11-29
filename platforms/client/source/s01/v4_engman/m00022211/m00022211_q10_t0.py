import pandas as pd
import pymysql.cursors
from pymongo import MongoClient
from dateutil.parser import parse
import direct_redis

# Mysql connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

mysql_query = """
SELECT n.N_NATIONKEY, n.N_NAME, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL,
c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
FROM nation n
JOIN customer c ON n.N_NATIONKEY = c.C_NATIONKEY;
"""
customer_data = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Filter orders from Q4 1993 to Q1 1994
orders_filter = {
    'O_ORDERDATE': {
        '$gte': '1993-10-01',
        '$lt': '1994-01-01'
    }
}
orders_cols = {'O_ORDERKEY': 1, 'O_CUSTKEY': 1}
orders_data = pd.DataFrame(list(mongo_db.orders.find(orders_filter, orders_cols)))
lineitem_data = pd.DataFrame(list(mongo_db.lineitem.find({}, {'_id': 0})))

mongo_client.close()

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer_keys = redis_client.keys(pattern='customer:*')
customers = [redis_client.get(key) for key in customer_keys]
customers_df = pd.DataFrame(customers)
redis_client.close()

# Combine data from MongoDB
combined_lineitem_orders = pd.merge(lineitem_data, orders_data, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate revenue lost
combined_lineitem_orders['revenue_lost'] = combined_lineitem_orders.apply(
    lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']), axis=1)

revenue_lost_sum = combined_lineitem_orders.groupby('O_CUSTKEY')['revenue_lost'].sum().reset_index()

# Merge with customer_data received from mysql
results = pd.merge(customer_data, revenue_lost_sum, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Sort the results
results_sorted = results.sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                                     ascending=[True, True, True, False])

# Select and rename columns
results_final = results_sorted[['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
results_final.columns = ['Customer Key', 'Customer Name', 'Revenue Lost', 'Account Balance', 'Nation', 'Address', 'Phone', 'Comment']

# Write to CSV
results_final.to_csv('query_output.csv', index=False)
