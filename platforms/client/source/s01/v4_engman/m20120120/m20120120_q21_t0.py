import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MySQL query to extract lineitems for orders with status 'F' and missing committed dates
mysql_query = """
SELECT 
    l.L_ORDERKEY, 
    l.L_SUPPKEY, 
    l.L_COMMITDATE, 
    l.L_RECEIPTDATE 
FROM 
    lineitem l
INNER JOIN 
    (SELECT 
        L_ORDERKEY 
     FROM 
        lineitem 
     GROUP BY 
        L_ORDERKEY 
     HAVING 
        COUNT(DISTINCT L_SUPPKEY) > 1) multi_l 
ON 
    l.L_ORDERKEY = multi_l.L_ORDERKEY
WHERE 
    l.L_RETURNFLAG = 'F' AND 
    l.L_COMMITDATE < l.L_RECEIPTDATE;
"""

# Execute the MySQL query and fetch results
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitems = cursor.fetchall()

# Convert the fetched data to a pandas DataFrame
lineitems_df = pd.DataFrame(lineitems, columns=['L_ORDERKEY', 'L_SUPPKEY', 'L_COMMITDATE', 'L_RECEIPTDATE'])

# Close MySQL connection
mysql_conn.close()

# Connect to Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the relevant tables from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
nation_df.columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']

supplier_df = pd.read_json(redis_conn.get('supplier'))
supplier_df.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']

# Close Redis connection
redis_conn.close()

# Filter out suppliers whose nation is SAUDI ARABIA
saudi_nation_key = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].iloc[0]
saudi_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'] == saudi_nation_key]

# Find the suppliers who were the only one failing to meet the delivery date for a multi-supplier order
late_suppliers_df = lineitems_df.merge(saudi_suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Count the number of times each supplier appears in the list of late suppliers
num_wait_count = late_suppliers_df.groupby(by=['S_NAME'])['L_ORDERKEY'].count().reset_index(name='NUMWAIT')

# Sort the result in descending order by NUMWAIT and ascending by S_NAME
result_df = num_wait_count.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
