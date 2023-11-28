import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL and retrieve nation data
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
    nation_data = cursor.fetchall()
nation_df = pd.DataFrame(nation_data, columns=['N_NATIONKEY', 'N_NAME'])
mysql_conn.close()

# Connect to MongoDB and retrieve orders and lineitem data with specified conditions
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']
lineitem_collection = mongodb['lineitem']

orders_query = {'O_ORDERDATE': {'$gte': '1993-10-01', '$lte': '1993-12-31'}}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query)))

lineitem_query = {'L_RETURNFLAG': 'R'}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query)))

mongo_client.close()

# Filter and join orders and lineitem data
orders_df = orders_df[['O_ORDERKEY', 'O_CUSTKEY']]
lineitem_df = lineitem_df[['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT']]
joined_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
joined_df['REVENUE'] = joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])

# Connect to Redis and retrieve customer data
r = direct_redis.DirectRedis(host='redis', port=6379)
customer_df = pd.read_json(r.get('customer'))

# Join data from all sources
result_df = pd.merge(customer_df, joined_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = pd.merge(result_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Aggregate and sort the results
grouped_result = result_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'],
    as_index=False
)['REVENUE'].sum()

grouped_result.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[True, True, True, False],
    inplace=True
)

# Write results to CSV
grouped_result.to_csv('query_output.csv', index=False)
