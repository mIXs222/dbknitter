import pandas as pd
import pymysql
from datetime import datetime
from direct_redis import DirectRedis

# Define the date range for the query
start_date = datetime(1990, 1, 1).date()
end_date = datetime(1995, 1, 1).date()

# Connect to MySQL and fetch relevant data
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT c.C_CUSTKEY, c.C_NATIONKEY 
        FROM customer as c 
        WHERE c.C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'))
    """)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NATIONKEY'])

    cursor.execute("""
        SELECT 
            o.O_ORDERKEY, o.O_CUSTKEY, 
            l.L_EXTENDEDPRICE, l.L_DISCOUNT 
        FROM orders AS o
        JOIN lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE o.O_ORDERDATE BETWEEN %s AND %s
    """, (start_date, end_date))
    lineitems = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Connect to Redis and fetch relevant data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_msgpack(redis_conn.get('nation'))
region_df = pd.read_msgpack(redis_conn.get('region'))
supplier_df = pd.read_msgpack(redis_conn.get('supplier'))

# Combine data from Redis to MySQL
asian_region_keys = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].astype(int)
asian_nation_keys = nation_df[nation_df['N_REGIONKEY'].isin(asian_region_keys)]['N_NATIONKEY'].astype(int)
asian_supplier_keys = supplier_df[supplier_df['S_NATIONKEY'].isin(asian_nation_keys)]['S_SUPPKEY'].astype(int)

# Merge customers and line items data
customer_lineitem = pd.merge(customers, lineitems, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Calculate revenue
customer_lineitem['REVENUE'] = customer_lineitem['L_EXTENDEDPRICE'] * (1 - customer_lineitem['L_DISCOUNT'])

# Group by Nation and calculate total revenue for eligible nations
result = customer_lineitem[customer_lineitem['C_NATIONKEY'].isin(asian_nation_keys)] \
    .groupby('C_NATIONKEY')['REVENUE'] \
    .sum() \
    .reset_index()

# Merge with nation names
result = result.merge(nation_df[['N_NATIONKEY', 'N_NAME']], left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and sort the final output
final_output = result[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write to CSV
final_output.to_csv('query_output.csv', index=False)
