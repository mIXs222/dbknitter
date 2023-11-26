import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT 
    customer.C_CUSTKEY, 
    customer.C_NAME,
    customer.C_ACCTBAL,
    customer.C_ADDRESS,
    customer.C_PHONE,
    customer.C_COMMENT,
    customer.C_NATIONKEY
FROM 
    customer
"""
mysql_cursor.execute(mysql_query)
customer_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'C_NATIONKEY'])

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

orders_col = mongo_db['orders']
orders_query = {'O_ORDERDATE': {'$gte': '1993-10-01', '$lt': '1994-01-01'}}
orders_projection = {'_id': False, 'O_CUSTKEY': True, 'O_ORDERKEY': True}
orders_data = pd.DataFrame(list(orders_col.find(orders_query, orders_projection)))

lineitem_col = mongo_db['lineitem']
lineitem_query = {'L_RETURNFLAG': 'R'}
lineitem_projection = {'_id': False, 'L_ORDERKEY': True, 'L_EXTENDEDPRICE': True, 'L_DISCOUNT': True}
lineitem_data = pd.DataFrame(list(lineitem_col.find(lineitem_query, lineitem_projection)))

# Redis connection and data retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_data = pd.read_json(redis_conn.get('nation'))

# Data manipulation and merging
orders_lineitem_merge = pd.merge(orders_data, lineitem_data, on='L_ORDERKEY')
orders_lineitem_merge['REVENUE'] = orders_lineitem_merge['L_EXTENDEDPRICE'] * (1 - orders_lineitem_merge['L_DISCOUNT'])
revenue_data = orders_lineitem_merge.groupby(['O_CUSTKEY'], as_index=False)['REVENUE'].sum()

final_merge = pd.merge(pd.merge(customer_data, revenue_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY'), nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Selecting and sorting the final output
final_columns = ['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
final_output = final_merge[final_columns].sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Writing to CSV
final_output.to_csv('query_output.csv', index=False)

# Clean up the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
