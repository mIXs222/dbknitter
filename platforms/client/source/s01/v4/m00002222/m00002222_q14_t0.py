# python_code.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_data():
    # Connect to the MySQL database
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )

    try:
        # Prepare SQL query to read data from MySQL
        mysql_query = 'SELECT * FROM part WHERE P_TYPE LIKE "PROMO%"'
        parts_df = pd.read_sql(mysql_query, mysql_conn)
    finally:
        mysql_conn.close()

    return parts_df

def get_redis_data():
    # Connect to the Redis database
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Read data from Redis
    lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

    return lineitem_df

# Get data from MySQL
parts_df = get_mysql_data()

# Get data from Redis
lineitem_df = get_redis_data()

# Filter lineitem based on the shipdate
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') &
                                   (lineitem_df['L_SHIPDATE'] < '1995-10-01')]

# Merge both dataframes on partkey
merged_df = pd.merge(filtered_lineitem_df, parts_df,
                     left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate PROMO_REVENUE
promo_revenue = (
    100.00 *
    sum(merged_df.query("P_TYPE.str.startswith('PROMO')")['L_EXTENDEDPRICE'] *
        (1 - merged_df.query("P_TYPE.str.startswith('PROMO')")['L_DISCOUNT'])) /
    sum(merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']))
)

# Save the output in a csv file
output_df = pd.DataFrame({'PROMO_REVENUE': [promo_revenue]})
output_df.to_csv('query_output.csv', index=False)
