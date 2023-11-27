import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
supplier_collection = mongodb['supplier']
customer_collection = mongodb['customer']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
def fetch_redis_data(table):
    return pd.read_json(redis_conn.get(table), orient='records')

# Query each database and collect relevant data
# MySQL
mysql_query = """
SELECT
    O_ORDERDATE,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_PARTKEY,
    L_SUPPKEY,
    O_ORDERKEY,
    O_CUSTKEY
FROM
    orders, lineitem
WHERE
    L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""
cursor.execute(mysql_query)
orders_lineitem_data = cursor.fetchall()

# MongoDB
supplier_data = list(supplier_collection.find())
customer_data = list(customer_collection.find())

# Redis
nation_data = fetch_redis_data('nation')
region_data = fetch_redis_data('region')
part_data = fetch_redis_data('part')

cursor.close()
mysql_conn.close()

# Convert to DataFrames
df_orders_lineitem = pd.DataFrame(orders_lineitem_data, columns=['O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_PARTKEY', 'L_SUPPKEY', 'O_ORDERKEY', 'O_CUSTKEY'])
df_supplier = pd.DataFrame(supplier_data)
df_customer = pd.DataFrame(customer_data)
df_nation = nation_data
df_region = region_data
df_part = part_data

# Filter out parts for 'P_TYPE = 'SMALL PLATED COPPER''
df_part = df_part[df_part['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge and calculate VOLUME
df = pd.merge(df_orders_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')
df['VOLUME'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

# Merge with customer and nation
df = df.merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df = df.merge(df_nation.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'NATION'}), on='C_NATIONKEY')

# Merge supplier and nation
df_supplier = df_supplier.merge(df_nation.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'NATION'}), on='S_NATIONKEY')
df = df.merge(df_supplier, on='L_SUPPKEY')

# Merge with region and filter for R_NAME = 'ASIA'
df_region_asia = df_region[df_region['R_NAME'] == 'ASIA']
df = df.merge(df_region_asia, left_on='C_NATIONKEY', right_on='R_REGIONKEY')

# Extract year and perform aggregation
df['O_YEAR'] = pd.to_datetime(df['O_ORDERDATE']).dt.year
df_result = df.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': (x.loc[x['NATION'] == 'INDIA', 'VOLUME'].sum()) / x['VOLUME'].sum()
    })
).reset_index()

# Write result to CSV
df_result.to_csv('query_output.csv', index=False)
