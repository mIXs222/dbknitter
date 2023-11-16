import pymysql
import pandas as pd
import direct_redis

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)

try:
    # Query to fetch parts from mysql
    part_query = """
    SELECT * FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
    """
    with mysql_conn.cursor() as cursor:
        cursor.execute(part_query)
        parts = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    # Query to fetch lineitems from Redis
    lineitem_df = redis_conn.get('lineitem')
    if lineitem_df is not None:
        lineitem_df = pd.read_json(lineitem_df)
    else:
        lineitem_df = pd.DataFrame()

    # Perform merge and filtering on the dataframes
    merged_df = pd.merge(lineitem_df, parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Compute the average quantity for part from lineitem
    avg_quantity = merged_df.groupby('P_PARTKEY')['L_QUANTITY'].mean().reset_index(name='AVG_QUANTITY')
    avg_quantity['LIMIT_QUANTITY'] = 0.2 * avg_quantity['AVG_QUANTITY']

    # Filter for the subquery condition on quantity
    final_df = merged_df[merged_df['L_QUANTITY'] < merged_df['P_PARTKEY'].map(avg_quantity.set_index('P_PARTKEY')['LIMIT_QUANTITY'])]

    # Calculate the result
    result_df = pd.DataFrame({'AVG_YEARLY': [(final_df['L_EXTENDEDPRICE'].sum() / 7.0)]})

    # Write the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

finally:
    # Close connections
    mysql_conn.close()
    redis_conn.close()
