import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Redis connection setup
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query the MySQL database
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(DISTINCT s.S_SUPPKEY) as supplier_count
            FROM 
                part p
            JOIN 
                supplier s ON p.P_PARTKEY = s.S_SUPPKEY
            WHERE 
                p.P_BRAND <> 'Brand#45'
                AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
                AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND s.S_COMMENT NOT LIKE '%Customer%Complaints%'
            GROUP BY 
                p.P_BRAND, p.P_TYPE, p.P_SIZE
            ORDER BY 
                supplier_count DESC, p.P_BRAND ASC, p.P_TYPE ASC, p.P_SIZE ASC
        """)
        mysql_rows = cursor.fetchall()
        mysql_columns = [desc[0] for desc in cursor.description]

# Load the partsupp table from Redis
redis_partsupp_df = pd.DataFrame(redis_conn.get('partsupp'), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Remove suppliers with Better Business Bureau complaints
filtered_partsupp_df = redis_partsupp_df[~redis_partsupp_df['PS_COMMENT'].str.contains('Customer Complaints')]

# Count suppliers for each part key
supplier_count_per_part = filtered_partsupp_df.groupby('PS_PARTKEY').size().reset_index(name='supplier_count')

# Combine MySQL and Redis data
combined_df = pd.DataFrame(mysql_rows, columns=mysql_columns)

# Merge the counts from Redis into the combined data frame
result_df = combined_df.merge(supplier_count_per_part, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='left')

# Reorder and rename columns
result_df = result_df[['P_BRAND', 'P_TYPE', 'P_SIZE', 'supplier_count_x']]
result_df.columns = ['brand', 'type', 'size', 'supplier_count']

# Write results to CSV
result_df.to_csv('query_output.csv', index=False)
