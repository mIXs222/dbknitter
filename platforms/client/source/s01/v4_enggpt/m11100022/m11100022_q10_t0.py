import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query MySQL for customer data
customer_query = """
SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT, C_NATIONKEY
FROM customer
"""
customer_df = pd.read_sql(customer_query, mysql_conn)
mysql_conn.close()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Query MongoDB for nation data
nation_df = pd.DataFrame(list(nation_collection.find({}, {'_id': 0})))

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for orders and lineitem data
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))
redis_conn.close()

# Filtering orders and lineitem for the specific time frame and return flag 'R'
filtered_orders_df = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-10-01') & 
    (orders_df['O_ORDERDATE'] <= '1993-12-31')
]
filtered_lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merge orders and lineitems
orders_lineitem_df = pd.merge(filtered_orders_df, filtered_lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate REVENUE
orders_lineitem_df['REVENUE'] = orders_lineitem_df['L_EXTENDEDPRICE'] * (1 - orders_lineitem_df['L_DISCOUNT'])

# Group by O_CUSTKEY and calculate total revenue
grouped_orders_lineitem_df = orders_lineitem_df.groupby('O_CUSTKEY').agg({'REVENUE': 'sum'}).reset_index()

# Merge with customers
result_df = pd.merge(grouped_orders_lineitem_df, customer_df, how='left', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Merge with nations
result_df = pd.merge(result_df, nation_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')
result_df = result_df.rename(columns={'N_NAME': 'NATION_NAME'})

# Select needed columns and sort
output_df = result_df[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'NATION_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]].sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
