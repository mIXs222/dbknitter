# Import necessary libraries
import pandas as pd
import pymysql
import pymongo
import direct_redis

# Function to query MySQL
def query_mysql():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4')
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT L_ORDERKEY, L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT
            FROM lineitem
            """
            cursor.execute(sql)
            lineitem_data = pd.DataFrame(cursor.fetchall(), columns=['L_ORDERKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
            
            sql = """
            SELECT R_REGIONKEY
            FROM region
            WHERE R_NAME = 'ASIA'
            """
            cursor.execute(sql)
            region_data = pd.DataFrame(cursor.fetchall(), columns=['R_REGIONKEY'])
            return lineitem_data, region_data
    finally:
        connection.close()

# Function to query MongoDB
def query_mongodb():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]
    customer_data = pd.DataFrame(list(db.customer.find({}, {'_id': 0})))
    return customer_data

# Function to query Redis
def query_redis():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_data = pd.read_json(r.get('nation'))
    supplier_data = pd.read_json(r.get('supplier'))
    orders_data = pd.read_json(r.get('orders'))
    return nation_data, supplier_data, orders_data

# Execute individual queries
lineitem_data, region_data = query_mysql()
customer_data = query_mongodb()
nation_data, supplier_data, orders_data = query_redis()

# Apply filters to orders and merge with region
orders_filtered = orders_data[(orders_data['O_ORDERDATE'] >= '1990-01-01') & (orders_data['O_ORDERDATE'] < '1995-01-01')]
region_orders = orders_filtered.merge(region_data, left_on='O_NATIONKEY', right_on='R_REGIONKEY')

# Merge the dataframes into one based on the conditions
df = customer_data.merge(region_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = df.merge(lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = df.merge(supplier_data, left_on=['L_SUPPKEY', 'C_NATIONKEY'], right_on=['S_SUPPKEY', 'S_NATIONKEY'])
df = df.merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by N_NAME and calculate REVENUE
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
result = df.groupby('N_NAME', as_index=False)['REVENUE'].sum()

# Sort the results and output to CSV
result_sorted = result.sort_values('REVENUE', ascending=False)
result_sorted.to_csv('query_output.csv', index=False)
