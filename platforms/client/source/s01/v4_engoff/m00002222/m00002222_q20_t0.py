import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Query to fetch necessary data from MySQL
mysql_query = """
SELECT S_SUPPKEY, S_NAME, COUNT(*) AS total_parts
FROM supplier
JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
JOIN lineitem ON supplier.S_SUPPKEY = lineitem.L_SUPPKEY
JOIN part ON lineitem.L_PARTKEY = part.P_PARTKEY
WHERE nation.N_NAME = 'CANADA'
AND part.P_NAME LIKE 'forest%'
AND lineitem.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
GROUP BY S_SUPPKEY, S_NAME
HAVING total_parts > 0.5 * (
    SELECT COUNT(*)
    FROM lineitem
    JOIN part ON lineitem.L_PARTKEY = part.P_PARTKEY
    WHERE part.P_NAME LIKE 'forest%'
)
"""
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Converting MySQL data to dataframe
supplier_excess_df = pd.DataFrame(mysql_results, columns=['S_SUPPKEY', 'S_NAME', 'TOTAL_PARTS'])

# DirectRedis connection and data-fetch
redis_conn = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.DataFrame(redis_conn.get('partsupp'))
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))

# Merge dataframes to check for excess of forest part
merged_df = lineitem_df.merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df['FOREST_PART'] = merged_df['PS_COMMENT'].str.contains('forest')
excess_df = merged_df.groupby('L_SUPPKEY').apply(lambda x: x[x['FOREST_PART']].sum())

# Write the result to CSV
final_df = supplier_excess_df.merge(excess_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
final_df.to_csv('query_output.csv', index=False)

# Close MySQL connection
mysql_conn.close()
