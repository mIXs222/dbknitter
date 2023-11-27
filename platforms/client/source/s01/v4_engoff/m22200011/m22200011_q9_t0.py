import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
conn_mysql = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
query_mysql = """SELECT s.S_NATIONKEY, YEAR(o.O_ORDERDATE) as year, 
                        SUM((l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
                 FROM supplier s
                 JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
                 JOIN lineitem l ON l.L_SUPPKEY = ps.PS_SUPPKEY
                 JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
                 WHERE l.L_PARTKEY = ps.PS_PARTKEY
                 GROUP BY s.S_NATIONKEY, YEAR(o.O_ORDERDATE)"""
df_mysql = pd.read_sql(query_mysql, conn_mysql)
conn_mysql.close()

# MongoDB connection and query
client_mongo = pymongo.MongoClient('mongodb', 27017)
db_mongo = client_mongo['tpch']
orders = list(db_mongo.orders.find({}, {"_id": 0, "O_ORDERKEY": 1, "O_ORDERDATE": 1}))
lineitem = list(db_mongo.lineitem.find({}, {"_id": 0, "L_ORDERKEY": 1, "L_SUPPKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1, "L_QUANTITY": 1}))
client_mongo.close()

# Convert to DataFrame and perform aggregation
df_orders = pd.DataFrame(orders)
df_lineitem = pd.DataFrame(lineitem)
merged_df = pd.merge(df_lineitem, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df['year'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']))

# Redis connection and query
conn_redis = DirectRedis(host='redis', port=6379, db=0)
# Fetch data from Redis and convert it
df_nation = pd.read_json(conn_redis.get('nation'), dtype={'N_NATIONKEY': int})
df_partsupp = pd.read_json(conn_redis.get('partsupp'))

# Merge DataFrames to calculate the final result
final_df = pd.merge(merged_df, df_partsupp, left_on='L_SUPPKEY', right_on='PS_SUPPKEY')
final_df = pd.merge(final_df, df_nation, left_on='N_NATIONKEY', right_on='S_NATIONKEY')
final_df['profit'] -= (final_df['PS_SUPPLYCOST'] * final_df['L_QUANTITY'])

# Calculate the result
result = final_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()
result.sort_values(by=['N_NAME', 'year'], ascending=[True, False], inplace=True)

# Write result to CSV
result.to_csv('query_output.csv', index=False)
