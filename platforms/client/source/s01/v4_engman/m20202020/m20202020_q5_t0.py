import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to calculate revenue
def calculate_revenue(row):
    return row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Get ASIA region key
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'")
    asia_region_key = cursor.fetchone()[0]

# Get nation keys that corresponds to ASIA
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY = %s", (asia_region_key,))
    asia_nation_keys = [item[0] for item in cursor.fetchall()]

# Get suppliers from ASIA
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN %s", (asia_nation_keys,))
    asia_supplier_keys = [item[0] for item in cursor.fetchall()]

# Get customers from ASIA
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY FROM customer WHERE C_NATIONKEY IN %s", (asia_nation_keys,))
    asia_customer_keys = [item[0] for item in cursor.fetchall()]

# Get lineitems with suppliers and customers from ASIA
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT l.L_EXTENDEDPRICE, l.L_DISCOUNT, c.C_NATIONKEY
        FROM lineitem l
        JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE c.C_CUSTKEY IN %s AND l.L_SUPPKEY IN %s
        AND l.L_SHIPDATE >= '1990-01-01' AND l.L_SHIPDATE < '1995-01-01'
        """, (asia_customer_keys, asia_supplier_keys))
    lineitems = cursor.fetchall()

# Convert lineitems to DataFrame
df_lineitems = pd.DataFrame(lineitems, columns=['L_EXTENDEDPRICE', 'L_DISCOUNT', 'C_NATIONKEY'])
df_lineitems['REVENUE'] = df_lineitems.apply(calculate_revenue, axis=1)

# Sum revenue by nation
df_revenue = df_lineitems.groupby(['C_NATIONKEY']).agg({'REVENUE': 'sum'}).reset_index()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get nation DataFrame from Redis
df_nations = pd.read_msgpack(redis_conn.get('nation'))

# Merge revenue with nation names
df_results = pd.merge(df_revenue, df_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='left')

# Final results with sorted revenue
df_final = df_results[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write final result to CSV
df_final.to_csv('query_output.csv', index=False)
