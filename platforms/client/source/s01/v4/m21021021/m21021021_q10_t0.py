# multi_source_query.py
import pandas as pd
import pymysql
import pymongo
import direct_redis
import csv

# Fetch data from MySQL
def fetch_mysql():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            sql = "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT, C_NATIONKEY FROM customer"
            cursor.execute(sql)
            rows = cursor.fetchall()
            customer_df = pd.DataFrame(list(rows), columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'C_NATIONKEY'])
    finally:
        connection.close()
    return customer_df

# Fetch data from MongoDB
def fetch_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client.tpch
    lineitem_collection = db.lineitem
    lineitem_cursor = lineitem_collection.find({'L_RETURNFLAG': 'R'}, {'_id': 0})
    lineitem_df = pd.DataFrame(list(lineitem_cursor))
    client.close()
    return lineitem_df

# Fetch data from Redis
def fetch_redis():
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_str = redis_client.get('nation')
    nation_df = pd.read_json(nation_str, orient='split')

    orders_str = redis_client.get('orders')
    orders_df = pd.read_json(orders_str, orient='split')
    return nation_df, orders_df

# Join and process the data frames
customer_df = fetch_mysql()
lineitem_df = fetch_mongodb()
nation_df, orders_df = fetch_redis()

# Convert order date to datetime and filter the range
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1993-10-01') & (orders_df['O_ORDERDATE'] < '1994-01-01')]

# Perform SQL joins and calculations as Pandas operations
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
grouped_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'], as_index=False)['REVENUE'].sum()

# Sort by the required fields
result_df = grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write the result to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
