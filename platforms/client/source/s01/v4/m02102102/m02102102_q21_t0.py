import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection information for MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection for Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load the tables from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation WHERE N_NAME = 'SAUDI ARABIA';")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM supplier;")
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    cursor.execute("SELECT * FROM orders WHERE O_ORDERSTATUS = 'F';")
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

mysql_conn.close()

# Load the lineitem table from Redis
lineitem = pd.read_json(redis_conn.get('lineitem'))

# Join the dataframes
merged_df = (
    suppliers.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(orders, left_on='S_SUPPKEY', right_on='O_CUSTKEY')
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Check the conditions
condition_1 = merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'] 

# Filter merged_df with condition_1
filtered_df = merged_df[condition_1]

# Get the main selection of columns for the final result
result_df = filtered_df[['S_NAME', 'L_ORDERKEY']]

# Calculate NUMWAIT
result_df = result_df.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Final sorting
result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
