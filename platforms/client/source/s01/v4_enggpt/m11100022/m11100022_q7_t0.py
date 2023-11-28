import pandas as pd
import pymysql
import pymongo
import direct_redis
from datetime import datetime
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME IN ('JAPAN', 'INDIA'));")
    supplier_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    cursor.execute("SELECT * FROM customer WHERE C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME IN ('JAPAN', 'INDIA'));")
    customer_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Retrieve data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find({'N_NAME': {'$in': ['JAPAN', 'INDIA']}})))

# Retrieve data from Redis
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter data for the timeframe of interest
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) & (lineitem_df['L_SHIPDATE'] <= datetime(1996, 12, 31))]

# Join dataframes to prepare for analysis
supp_nation_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
cust_nation_df = customer_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue for line items
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Join tables
result_df = (lineitem_df
             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(supp_nation_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(cust_nation_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'))

# Filter for nations of interest and group by
filtered_result_df = result_df[
    ((result_df['N_NAME_x'] == 'JAPAN') & (result_df['N_NAME_y'] == 'INDIA')) |
    ((result_df['N_NAME_x'] == 'INDIA') & (result_df['N_NAME_y'] == 'JAPAN'))
]
grouped_result = filtered_result_df.groupby(['N_NAME_x', 'N_NAME_y', lineitem_df['L_SHIPDATE'].dt.year])['REVENUE'].sum().reset_index()

# Order the results
ordered_result = grouped_result.sort_values(by=['N_NAME_x', 'N_NAME_y', 'L_SHIPDATE'])
ordered_result.columns = ['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR_OF_SHIPPING', 'REVENUE']

# Write to CSV file
ordered_result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.connection_pool.disconnect()
