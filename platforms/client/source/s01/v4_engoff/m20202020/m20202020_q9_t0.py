import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

# Run query on MySQL
mysql_query = """
SELECT
    s.S_NATIONKEY,
    YEAR(l.L_SHIPDATE) as year,
    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
FROM
    supplier s
JOIN
    lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
JOIN
    partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
WHERE
    l.L_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%dim%')
GROUP BY
    s.S_NATIONKEY, year
ORDER BY
    s.S_NATIONKEY, year DESC;
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
part_df = pd.read_json(redis_conn.get('part'))
part_df = part_df[part_df['P_NAME'].str.contains("dim")]
partsupp_df = pd.read_json(redis_conn.get('partsupp'))

# Filter partsupp_df for parts with "dim" in the name
partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Merge Redis dataframes
redis_df = nation_df.merge(partsupp_df, left_on='N_NATIONKEY', right_on='PS_SUPPKEY')

# Merge MySQL and Redis results
result_df = pd.merge(mysql_df, redis_df, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Write the combined dataframe to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
