import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
with mysql_conn.cursor() as cursor:
    # Retrieve relevant data from lineitem and region
    cursor.execute("""
        SELECT 
            l.L_ORDERKEY,
            l.L_EXTENDEDPRICE,
            l.L_DISCOUNT,
            l.L_SHIPDATE
        FROM lineitem l
    """)
    lineitem_data = cursor.fetchall()
    lineitem_df = pd.DataFrame(lineitem_data, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE'])

    cursor.execute("""
        SELECT 
            r.R_REGIONKEY, 
            r.R_NAME
        FROM region r
        WHERE r.R_NAME = 'ASIA'
    """)
    region_data = cursor.fetchall()
    region_df = pd.DataFrame(region_data, columns=['R_REGIONKEY', 'R_NAME'])

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']
customer_data = list(customer_collection.find())
customer_df = pd.DataFrame(customer_data).drop(columns=['_id'])

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_client.get('supplier'))
nation_df = pd.read_json(redis_client.get('nation'))
orders_df = pd.read_json(redis_client.get('orders'))

# Filter orders by date range
start_date = datetime(1990, 1, 1)
end_date = datetime(1994, 12, 31)
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Join tables
result_df = (orders_df
             .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
             .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
             .merge(supplier_df, left_on='O_ORDERKEY', right_on='S_SUPPKEY')
             .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY'))

# Filter for ASIA region only
result_df = result_df[result_df['R_NAME'] == 'ASIA']

# Calculate the revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Grouping by nation and sum up the revenue
final_result_df = (result_df.groupby('N_NAME')['REVENUE']
                   .sum()
                   .reset_index()
                   .sort_values('REVENUE', ascending=False))

# Write to CSV
final_result_df.to_csv('query_output.csv', index=False)
