# suppliers_from_canada.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL
def mysql_connection():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
    )
    return connection

# Function to execute MySQL query
def mysql_query(query, connection):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return data

# Function to connect to MongoDB
def mongodb_connection():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    return db

# Function to query data from Redis
def redis_query(table_name):
    redis_connection = DirectRedis(host='redis', port=6379, db=0)
    return pd.read_json(redis_connection.get(table_name))

# Connect to MySQL and get nation and part data where applicable
mysql_conn = mysql_connection()
query_nation = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'CANADA';"
nation_data = mysql_query(query_nation, mysql_conn)

canadian_nation_key = nation_data[0][0] if nation_data else None

# Connect to MongoDB and get lineitem data
mongodb_conn = mongodb_connection()
lineitem_data = list(mongodb_conn.lineitem.find(
    {"L_SHIPDATE": {"$gte": "1994-01-01", "$lte": "1995-01-01"}}
))

# Connect to Redis and get supplier and partsupp data
supplier_df = redis_query('supplier')
partsupp_df = redis_query('partsupp')

# Close MySQL connection
mysql_conn.close()

# Filter partsupp based on forest condition
forest_partsupp = partsupp_df[partsupp_df['PS_COMMENT'].str.startswith('forest')]

# Calculate threshold quantity
thresholds = {}
for ps in forest_partsupp.itertuples():
    partkey = ps.PS_PARTKEY
    suppkey = ps.PS_SUPPKEY
    total_qty = sum(item['L_QUANTITY'] for item in lineitem_data if item['L_PARTKEY'] == partkey and item['L_SUPPKEY'] == suppkey)
    thresholds[(partkey, suppkey)] = total_qty * 0.5

filtered_partsupp = set(
    (ps.PS_PARTKEY, ps.PS_SUPPKEY) for ps in forest_partsupp.itertuples() if thresholds.get((ps.PS_PARTKEY, ps.PS_SUPPKEY), 0) < ps.PS_AVAILQTY
)

# Filter suppliers by Canadian nation key and partsupp
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == canadian_nation_key]
supplier_df = supplier_df[supplier_df['S_SUPPKEY'].isin([suppkey for _, suppkey in filtered_partsupp])]

# Select supplier names and addresses, sort them by name
supplier_df = supplier_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')
supplier_df.to_csv('query_output.csv', index=False)
