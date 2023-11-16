import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to mysql and retrieve orders data
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
try:
    with mysql_conn.cursor() as cursor:
        mysql_query = """
        SELECT
            O_ORDERKEY,
            O_CUSTKEY,
            O_ORDERDATE,
            O_SHIPPRIORITY
        FROM
            orders
        WHERE
            O_ORDERDATE < '1995-03-15'
        """
        cursor.execute(mysql_query)
        orders_data = cursor.fetchall()
finally:
    mysql_conn.close()

# Turn orders_data into a DataFrame
orders_df = pd.DataFrame(orders_data, columns=[
    'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem data from Redis
lineitem_df = pd.read_json(redis.get('lineitem'))
lineitem_df['REVENUE'] = lineitem_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Retrieve customer data from Redis
customer_df = pd.read_json(redis.get('customer'))

# Perform the filtering and join operations
filtered_customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
merged_df = filtered_customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the date filters
final_df = final_df[final_df['O_ORDERDATE'] < '1995-03-15']
final_df = final_df[final_df['L_SHIPDATE'] > '1995-03-15']

# Group by the necessary fields
grouped_df = final_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({
    'REVENUE': 'sum'
}).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write output to CSV
sorted_df.to_csv('query_output.csv', index=False)
