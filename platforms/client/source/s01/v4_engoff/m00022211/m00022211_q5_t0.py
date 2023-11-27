import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA')")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])
mysql_conn.close()

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_coll = mongo_db['orders']
lineitem_coll = mongo_db['lineitem']
orders_df = pd.DataFrame(list(orders_coll.find(
    {"O_ORDERDATE": {"$gte": "1990-01-01", "$lt": "1995-01-01"}}
)))
lineitem_df = pd.DataFrame(list(lineitem_coll.find()))

# Redis connection using direct_redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_client.get('supplier'))
customer_df = pd.read_json(redis_client.get('customer'))

# Filtering Redis data for only ASIA suppliers and customers
asia_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(nations['N_NATIONKEY'])]
asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(nations['N_NATIONKEY'])]

# Merging MongoDB and Redis data
order_lineitem_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
asia_orders = order_lineitem_df[order_lineitem_df['O_CUSTKEY'].isin(asia_customers['C_CUSTKEY'])]

# Calculating revenue
asia_orders['REVENUE'] = asia_orders['L_EXTENDEDPRICE'] * (1 - asia_orders['L_DISCOUNT'])
revenue_df = asia_orders[['L_ORDERKEY', 'REVENUE']]

# Combining with nations
result_df = pd.merge(revenue_df, asia_customers[['C_CUSTKEY', 'C_NATIONKEY']], left_on='L_ORDERKEY', right_on='C_CUSTKEY')
result_df = pd.merge(result_df, asia_suppliers[['S_SUPPKEY', 'S_NATIONKEY']], left_on='C_NATIONKEY', right_on='S_NATIONKEY')
result_df = pd.merge(result_df, nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Grouping by nation and summing revenue
final_result_df = result_df.groupby('N_NAME')['REVENUE'].sum().reset_index()
final_result_df = final_result_df.sort_values('REVENUE', ascending=False)

# Writing to CSV file
final_result_df.to_csv('query_output.csv', index=False)
