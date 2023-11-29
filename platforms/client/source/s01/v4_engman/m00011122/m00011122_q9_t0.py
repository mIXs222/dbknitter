import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    query_mysql = """
    SELECT n.N_NAME AS nation, YEAR(o.O_ORDERDATE) AS year,
    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
    FROM nation n
    JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN partsupp ps ON ps.PS_SUPPKEY = s.S_SUPPKEY
    JOIN lineitem l ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = s.S_SUPPKEY
    JOIN orders o ON o.O_ORDERKEY = l.L_ORDERKEY
    JOIN part p ON p.P_PARTKEY = l.L_PARTKEY
    WHERE p.P_NAME LIKE '%dim%'
    GROUP BY nation, year
    ORDER BY nation ASC, year DESC;
    """
    cursor.execute(query_mysql)
    result_mysql = cursor.fetchall()

mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
supplier_col = mongodb["supplier"]
partsupp_col = mongodb["partsupp"]

supplycost_dict = {
    (doc["PS_PARTKEY"], doc["PS_SUPPKEY"]): doc["PS_SUPPLYCOST"]
    for doc in partsupp_col.find({}, {"PS_PARTKEY": 1, "PS_SUPPKEY": 1, "PS_SUPPLYCOST": 1})
}

mongo_client.close()

# Redis connection and data retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_msgpack(redis_conn.get('orders'))
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Data Processing and merging
lineitem_df["PS_SUPPLYCOST"] = lineitem_df.apply(lambda x: supplycost_dict.get((x["L_PARTKEY"], x["L_SUPPKEY"]), 0), axis=1)
merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Applying profit calculation
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])
merged_df['year'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year

# Aggregation by nation and year for profit
profit_df = merged_df.groupby(['nation', 'year']).agg({'profit': 'sum'}).reset_index()
profit_df = profit_df.sort_values(by=['nation', 'year'], ascending=[True, False])

final_result_df = pd.DataFrame(result_mysql, columns=['nation', 'year', 'profit']).append(profit_df, ignore_index=True)

# Write to query_output.csv
final_result_df.to_csv('query_output.csv', index=False)
