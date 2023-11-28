import pandas as pd
import pymysql
import direct_redis

# Connection information
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    db=mysql_config['db']
)

# Prepare the query for "lineitem" on MySQL
lineitem_query = """
SELECT
    L_SUPPKEY as SUPPLIER_NO,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
GROUP BY
    L_SUPPKEY
"""

# Execute query on MySQL
lineitem_data = pd.read_sql(lineitem_query, con=mysql_conn)
lineitem_data['TOTAL_REVENUE'] = lineitem_data['TOTAL_REVENUE'].astype('float')
# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Fetch the "supplier" table from Redis
supplier_data_df = pd.read_json(redis_conn.get('supplier'), orient='records')

# Merge dataframes based on SUPPLIER_NO and S_SUPPKEY
merged_data = pd.merge(supplier_data_df, lineitem_data, left_on='S_SUPPKEY', right_on='SUPPLIER_NO', how='inner')

# Identify the supplier with maximum total revenue
max_revenue_supplier = merged_data.loc[merged_data['TOTAL_REVENUE'].idxmax()]

# Format the output dataframe
output_df = max_revenue_supplier[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']].to_frame().transpose().sort_values('S_SUPPKEY')

# Write the final output to CSV
output_df.to_csv('query_output.csv', index=False)
