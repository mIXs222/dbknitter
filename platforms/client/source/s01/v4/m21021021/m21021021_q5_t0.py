from pymongo import MongoClient
import pymysql
import pandas as pd
import direct_redis

# MySQL connection and query execution
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    query = """
    SELECT C_CUSTKEY, C_NATIONKEY
    FROM customer
    """
    mysql_data = pd.read_sql(query, connection)
    connection.close()
    return mysql_data

# MongoDB connection and fetching documents
def fetch_mongodb_data():
    client = MongoClient('mongodb', 27017)
    db = client.tpch
    region_data = pd.DataFrame(list(db.region.find({'R_NAME': 'ASIA'})))
    lineitem_data = pd.DataFrame(list(db.lineitem))
    client.close()
    return region_data, lineitem_data

# Redis connection and fetching tables
def fetch_redis_data():
    client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_data = pd.read_json(client.get('nation'))
    supplier_data = pd.read_json(client.get('supplier'))
    orders_data = pd.read_json(client.get('orders'))
    return nation_data, supplier_data, orders_data

# Fetch data from all databases
mysql_data = fetch_mysql_data()
region_data, lineitem_data = fetch_mongodb_data()
nation_data, supplier_data, orders_data = fetch_redis_data()

# Perform filtering on dates within the orders data
orders_data = orders_data[(orders_data['O_ORDERDATE'] >= '1990-01-01') &
                          (orders_data['O_ORDERDATE'] < '1995-01-01')]

# Merge dataframes to simulate JOINs
df = pd.merge(mysql_data, orders_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, supplier_data, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
df = pd.merge(df, nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = pd.merge(df, region_data, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Perform the required calculations
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
result = df.groupby('N_NAME')['REVENUE'].sum().reset_index()
result = result.sort_values(by='REVENUE', ascending=False)

# Write the final result to a CSV file
result.to_csv('query_output.csv', index=False)
