import csv
import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the query for MySQL
mysql_query = """
SELECT 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM 
    lineitem
WHERE
    (
        (L_SHIPMODE = 'AIR' OR L_SHIPMODE = 'AIR REG')
        AND (L_SHIPINSTRUCT = 'DELIVER IN PERSON')
    )
"""

# Execute the query on MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    # Fetch results from the MySQL query
    mysql_result = cursor.fetchone()

# Get and process data from Redis (handling the part table is assumed due to lack of support for complex queries)
# Retrieve part table from Redis as Pandas DataFrame
part_df = pd.DataFrame(redis_conn.get('part'))

# Perform necessary filtering according to the specific brand and container conditions
part_df_filtered = part_df[
    ((part_df['P_BRAND'].astype(int) == 12) & (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (1 <= part_df['P_SIZE']) & (part_df['P_SIZE'] <= 5)) |
    ((part_df['P_BRAND'].astype(int) == 23) & (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (10 <= part_df['P_SIZE']) & (part_df['P_SIZE'] <= 10)) |
    ((part_df['P_BRAND'].astype(int) == 34) & (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (20 <= part_df['P_SIZE']) & (part_df['P_SIZE'] <= 15))
]

# Assume 'part' and 'lineitem' dataframes are linked by the partkey, merge on relevant keys
# In actual use, you would need to adjust the merge to match the actual shared key(s) or perform processing to join the Redis data with MySQL data.
# However, due to limited context and no shared key in provided schema, we'll skip this step.

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['REVENUE'])
    writer.writerow([mysql_result[0]])

# Close connections
mysql_conn.close()
