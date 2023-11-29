import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get part and supplier data from Redis
part_data = eval(redis_conn.get('part'))
supplier_data = eval(redis_conn.get('supplier'))
nation_data = eval(redis_conn.get('nation'))

# Convert Redis data to pandas dataframes
df_part = pd.DataFrame(part_data)
df_supplier = pd.DataFrame(supplier_data)
df_nation = pd.DataFrame(nation_data)

# Query to select parts like forest from the MySQL database
mysql_query_parts = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
WHERE PS_PARTKEY IN (
    SELECT P_PARTKEY
    FROM lineitem
    INNER JOIN (SELECT * FROM part WHERE P_NAME LIKE 'forest%') AS forest_parts
    ON lineitem.L_PARTKEY = forest_parts.P_PARTKEY
    AND L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
)
AND PS_AVAILQTY > (SELECT 0.5 * SUM(L_QUANTITY)
                    FROM lineitem
                    WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
                    AND L_PARTKEY IN (SELECT P_PARTKEY
                                      FROM part
                                      WHERE P_NAME LIKE 'forest%')
                    )
"""

mysql_cursor.execute(mysql_query_parts)
partsupp_results = mysql_cursor.fetchall()
df_partsupp = pd.DataFrame(partsupp_results, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Join the pandas dataframes
df_merged = pd.merge(df_partsupp, df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_merged = df_merged[df_merged['S_NATIONKEY'] == df_nation[df_nation['N_NAME'] == 'CANADA']['N_NATIONKEY'].values[0]]
df_merged = df_merged[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']]

# Close connections
mysql_cursor.close()
mysql_conn.close()

# Write the results to a CSV file
df_merged.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
