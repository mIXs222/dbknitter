import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Functions to get data from Redis
def get_df_from_redis(key):
    return pd.read_json(redis_client.get(key).decode('utf-8'))

# MySQL query
mysql_query = """
SELECT
    O_ORDERDATE,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY
FROM
    orders
JOIN
    lineitem ON O_ORDERKEY = L_ORDERKEY
"""

# Execute MySQL query and fetch the data
mysql_cursor.execute(mysql_query)
orders_lineitem_data = mysql_cursor.fetchall()
mysql_cursor.close()
mysql_conn.close()

# Convert to DataFrame
orders_lineitem_columns = ['O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY']
orders_lineitem_df = pd.DataFrame(orders_lineitem_data, columns=orders_lineitem_columns)

# Get MongoDB collections
supplier_coll = mongo_db['supplier']
partsupp_coll = mongo_db['partsupp']

# Convert MongoDB collections to DataFrames
supplier_df = pd.DataFrame(list(supplier_coll.find({})))
partsupp_df = pd.DataFrame(list(partsupp_coll.find({})))

# Redis DataFrames
nation_df = get_df_from_redis('nation')
part_df = get_df_from_redis('part')

# Merge that mimics the joins in the SQL query
merged_df = orders_lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(partsupp_df, left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])
merged_df = merged_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter by P_NAME like '%dim%'
merged_df = merged_df[merged_df['P_NAME'].str.contains('dim')]

# Calculate AMOUNT
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Aggregate and Summarize
result_df = merged_df.groupby(['N_NAME', merged_df['O_ORDERDATE'].dt.year])['AMOUNT'].sum().reset_index()
result_df.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

# Sort
result_df = result_df.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False])

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
