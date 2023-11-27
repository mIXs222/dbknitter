# query.py
import csv
import pandas as pd
import pymysql
import pymongo
from datetime import datetime
import direct_redis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis Connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    query_supplier = 'SELECT * FROM supplier'
    cursor.execute(query_supplier)
    supplier_data = cursor.fetchall()
    
    query_customer = 'SELECT * FROM customer'
    cursor.execute(query_customer)
    customer_data = cursor.fetchall()

# Convert to DataFrames
supplier_df = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

# Retrieve data from MongoDB
orders_data = list(mongodb_db.orders.find())
lineitem_data = list(mongodb_db.lineitem.find())

# Convert to DataFrames
orders_df = pd.DataFrame(orders_data)
lineitem_df = pd.DataFrame(lineitem_data)

# Retrieve data from Redis
nation_data = pd.read_json(redis_conn.get('nation').decode('utf-8'))
region_data = pd.read_json(redis_conn.get('region').decode('utf-8'))
part_data = pd.read_json(redis_conn.get('part').decode('utf-8'))

# Convert nation names to uppercase as required for comparison
nation_df = nation_data.copy()
nation_df['N_NAME'] = nation_df['N_NAME'].str.upper()

# Perform query
merged_df = (lineitem_df
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'N1_NATIONKEY'}), left_on='C_NATIONKEY', right_on='N1_NATIONKEY')
    .merge(region_data, left_on='N1_NATIONKEY', right_on='R_REGIONKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'N2_NATIONKEY'}), left_on='S_NATIONKEY', right_on='N2_NATIONKEY')
    .merge(part_data, on='P_PARTKEY'))

asia_df = merged_df[(merged_df['R_NAME'].str.upper() == 'ASIA') &
                    (merged_df['O_ORDERDATE'] >= datetime(1995, 1, 1)) &
                    (merged_df['O_ORDERDATE'] <= datetime(1996, 12, 31)) &
                    (merged_df['P_TYPE'] == 'SMALL PLATED COPPER')]

asia_df['VOLUME'] = asia_df['L_EXTENDEDPRICE'] * (1 - asia_df['L_DISCOUNT'])
asia_df['O_YEAR'] = asia_df['O_ORDERDATE'].dt.year

# Calculate market share
final_df = asia_df.groupby('O_YEAR').apply(lambda x: pd.Series({
    'MKT_SHARE': (x[x['N2_NATIONKEY'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum())
})).reset_index()

# Write to CSV
final_df.to_csv('query_output.csv', index=False)

# Close Connections
mysql_conn.close()
mongodb_client.close()
redis_conn.connection_pool.disconnect()
