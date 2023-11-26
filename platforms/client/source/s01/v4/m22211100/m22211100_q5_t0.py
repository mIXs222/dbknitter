# query_data.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# MongoDB client
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("""
    SELECT
        o.O_ORDERKEY, o.O_CUSTKEY, o.O_ORDERDATE,
        l.L_ORDERKEY, l.L_SUPPKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT
    FROM
        orders o JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    WHERE 
        o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE < '1995-01-01'
    """)
    
    orders_lineitem_data = cursor.fetchall()
    
mysql_df = pd.DataFrame(orders_lineitem_data, columns=[
    'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE',
    'L_ORDERKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Load data from MongoDB
supplier_data = list(mongo_db.supplier.find({}, {'_id': 0}))
customer_data = list(mongo_db.customer.find({}, {'_id': 0}))

supplier_df = pd.DataFrame(supplier_data)
customer_df = pd.DataFrame(customer_data)

# Load data from Redis
nation_data = redis_client.get('nation')
region_data = redis_client.get('region')

nation_df = pd.read_json(nation_data, orient='records')
region_df = pd.read_json(region_data, orient='records')

# Join all dataframes
df = pd.merge(customer_df, orders_lineitem_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, supplier_df, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
df = pd.merge(df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = pd.merge(df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter for orders from the 'ASIA' region
df = df[df['R_NAME'] == 'ASIA']

# Calculate revenue
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

# Group by nation name and calculate sum of revenue
result = df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sorting the result
result = result.sort_values('REVENUE', ascending=False)

# Write to CSV
result.to_csv('query_output.csv', index=False)

# Close all connections
mysql_connection.close()
mongo_client.close()
redis_client.close()
