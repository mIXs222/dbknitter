# query_exec.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            L_PARTKEY,
            L_QUANTITY,
            L_EXTENDEDPRICE        
        FROM
            lineitem
    """)
    lineitem_data = cursor.fetchall()

# Convert fetched data to pandas DataFrame
lineitem_df = pd.DataFrame(list(lineitem_data), columns=['L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE'])

# Close MySQL connection
mysql_conn.close()

# Redis connection setup
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get part data from Redis
part_data_json = redis_conn.get('part')
part_df = pd.read_json(part_data_json)

# Filter operation similar to SQL JOIN and WHERE conditions on part DataFrame
part_filtered = part_df[
    (part_df['P_BRAND'] == 'Brand#23') &
    (part_df['P_CONTAINER'] == 'MED BAG')
]

# Filter operation similar to SQL subquery and JOIN on lineitem DataFrame
lineitem_filtered = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_filtered['P_PARTKEY'])]
lineitem_avg = lineitem_filtered.groupby('L_PARTKEY')['L_QUANTITY'].transform('mean')
lineitem_final = lineitem_filtered[lineitem_filtered['L_QUANTITY'] < 0.2 * lineitem_avg]

# Calculation of AVG_YEARLY
result_df = pd.DataFrame({
    'AVG_YEARLY': [lineitem_final['L_EXTENDEDPRICE'].sum() / 7.0]
})

# Writing the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
