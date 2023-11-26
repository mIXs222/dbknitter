import pymysql
import pymongo
from datetime import datetime
import csv
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute("""
        SELECT
            S.S_NATIONKEY,
            L.L_ORDERKEY,
            L.L_PARTKEY,
            L.L_SUPPKEY,
            L.L_EXTENDEDPRICE,
            L.L_DISCOUNT,
            PS.PS_SUPPLYCOST,
            L.L_QUANTITY
        FROM
            lineitem L,
            partsupp PS,
            supplier S
        WHERE
            S.S_SUPPKEY = L.L_SUPPKEY
            AND PS.PS_SUPPKEY = L.L_SUPPKEY
            AND PS.PS_PARTKEY = L.L_PARTKEY
    """)
    mysql_data = mysql_cursor.fetchall()

# Query MongoDB for nation and orders
orders = pd.DataFrame(list(mongodb_db.orders.find()))  # Orders data
nation = pd.DataFrame(list(mongodb_db.nation.find()))  # Nation data

# Query Redis for parts
part_df = pd.read_json(redis_client.get('part'), orient='records')

# Filtering MongoDB and Redis tables
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders = orders[orders['O_ORDERDATE'].dt.strftime('%Y-%m') != '']
part_df = part_df[part_df['P_NAME'].str.contains('dim')]

# Merge MySQL data into a DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=[
    'S_NATIONKEY', 'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'PS_SUPPLYCOST', 'L_QUANTITY'
])

# Perform the join operations for all data
merge_df = pd.merge(
    pd.merge(
        pd.merge(
            pd.merge(orders, nation, how='inner', left_on='O_ORDERKEY', right_on='N_NATIONKEY'),
            mysql_df, how='inner', left_on=['O_ORDERKEY', 'N_NATIONKEY'], right_on=['L_ORDERKEY', 'S_NATIONKEY']
        ),
        part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY'
    ),
    mysql_df, how='inner', on=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY']
)

# Calculate the AMOUNT
merge_df['AMOUNT'] = merge_df['L_EXTENDEDPRICE'] * (1 - merge_df['L_DISCOUNT']) - merge_df['PS_SUPPLYCOST'] * merge_df['L_QUANTITY']

# Final aggregation and grouping
result_df = merge_df.groupby(['N_NAME', orders['O_ORDERDATE'].dt.year.rename('O_YEAR')]).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Sort the result
result_df.sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False], inplace=True)

# Save results to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
