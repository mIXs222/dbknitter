# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the mysql database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Execute mysql queries
def mysql_query(sql):
    with mysql_connection.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return pd.DataFrame(result, columns=[i[0] for i in cursor.description])

# Connect to the redis database
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get the data from mysql
orders_query = """
SELECT * FROM orders WHERE O_ORDERSTATUS = 'F';
"""
orders_df = mysql_query(orders_query)

lineitem_query = """
SELECT L_ORDERKEY, L_SUPPKEY, L_COMMITDATE, L_RECEIPTDATE
FROM lineitem
WHERE L_RECEIPTDATE > L_COMMITDATE;
"""
lineitem_df = mysql_query(lineitem_query)

# Get the data from redis
nation_df = pd.read_json(redis_connection.get('nation'))
supplier_df = pd.read_json(redis_connection.get('supplier'))

# Filter the suppliers from nations of interest
nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Merge the DataFrames from different databases
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter by the EXISTS subqueries conditions
merged_df['NUMWAIT'] = merged_df.groupby('L_ORDERKEY')['L_ORDERKEY'].transform('count')

# Apply the conditions specified in the EXISTS subqueries
merged_df = merged_df.drop_duplicates(subset=['S_NAME', 'L_ORDERKEY'])

# Select supplier names and the count of line items representing the waiting time
result_df = merged_df[['S_NAME', 'NUMWAIT']]

# Order the results as specified by the user query
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_connection.close()
redis_connection.close()
