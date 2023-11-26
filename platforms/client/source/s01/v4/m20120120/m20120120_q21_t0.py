# query_exec.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query for getting data from `lineitem` table
lineitem_query = """
SELECT 
    L_ORDERKEY, L_SUPPKEY, L_COMMITDATE, L_RECEIPTDATE 
FROM 
    lineitem 
WHERE 
    L_RECEIPTDATE > L_COMMITDATE
"""

# Retrieve data from MySQL
with connection.cursor() as cursor:
    cursor.execute(lineitem_query)
    lineitem_result = cursor.fetchall()

# Build a DataFrame for the lineitem table
df_lineitem = pd.DataFrame(list(lineitem_result), columns=['L_ORDERKEY', 'L_SUPPKEY', 'L_COMMITDATE', 'L_RECEIPTDATE'])

# Connect to the Redis database
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
df_supplier = pd.read_json(redis.get('supplier'))
df_orders = pd.read_json(redis.get('orders'))
df_nation = pd.read_json(redis.get('nation'))

# Filter data for `SAUDI ARABIA` and `O_ORDERSTATUS = 'F'`
df_nation = df_nation[df_nation['N_NAME'] == 'SAUDI ARABIA']
df_orders = df_orders[df_orders['O_ORDERSTATUS'] == 'F']

# Join the data
df_lineitem_orders = pd.merge(df_lineitem, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_lineitem_orders_suppliers = pd.merge(df_lineitem_orders, df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_final = pd.merge(df_lineitem_orders_suppliers, df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by `S_NAME`
df_final = df_final.groupby('S_NAME').agg(NUMWAIT=pd.NamedAgg(column='S_NAME', aggfunc='count')).reset_index()

# Sort the results
df_final = df_final.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output the results to CSV
df_final.to_csv('query_output.csv', index=False)

# Close database connection
connection.close()

