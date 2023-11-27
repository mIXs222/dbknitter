import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT 
            O_CUSTKEY, 
            O_ORDERKEY 
        FROM orders
        WHERE 
            O_ORDERDATE >= '1993-10-01' AND 
            O_ORDERDATE < '1994-01-01'
    """)
    orders_data = cursor.fetchall()
    
    cursor.execute("""
        SELECT 
            N_NATIONKEY, 
            N_NAME
        FROM nation
    """)
    nations_data = cursor.fetchall()

# Transform MySQL data into pandas DataFrames
orders_df = pd.DataFrame(orders_data, columns=['O_CUSTKEY', 'O_ORDERKEY'])
nations_df = pd.DataFrame(nations_data, columns=['N_NATIONKEY', 'N_NAME'])

# Retrieve and transform Redis data into pandas DataFrame
lineitem_str = redis_client.get('lineitem')
lineitem_df = pd.read_msgpack(lineitem_str)
lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Retrieve and transform MongoDB data into pandas DataFrame
customer_data = list(mongodb.customer.find({}))
customer_df = pd.DataFrame.from_records(customer_data).drop('_id', axis=1)

# Perform the join operations
merged_df = (
    orders_df.merge(
        lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY'
    )
    .merge(
        customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'
    )
    .merge(
        nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY'
    )
)

# Calculate 'REVENUE' and perform GROUP BY
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
result_df = (
    merged_df
    .groupby(
        [
            'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 
            'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'
        ]
    )
    ['REVENUE']
    .sum()
    .reset_index()
)

# Sort the results
result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
