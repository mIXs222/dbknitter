import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query MySQL for relevant data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            s.S_SUPPKEY,
            n.N_NAME,
            r.R_NAME,
            p.P_TYPE,
            p.P_SIZE,
            l.L_EXTENDEDPRICE,
            l.L_DISCOUNT,
            l.L_SHIPDATE
        FROM
            supplier s
            JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
            JOIN part p ON p.P_TYPE = 'SMALL PLATED COPPER'
        WHERE
            n.N_NAME = 'INDIA'
            AND r.R_NAME = 'ASIA'
    """)
    suppliers_parts = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'N_NAME', 'R_NAME', 'P_TYPE', 'P_SIZE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE'])

mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for relevant data
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Combine data from MySQL and Redis
combined_df = pd.merge(suppliers_parts, lineitem_df, on='S_SUPPKEY')

# Calculate revenue for 1995 and 1996
combined_df['L_SHIPDATE'] = pd.to_datetime(combined_df['L_SHIPDATE'])
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

final_df = combined_df[(combined_df['L_SHIPDATE'].dt.year == 1995) | (combined_df['L_SHIPDATE'].dt.year == 1996)]
final_df = final_df.groupby(combined_df['L_SHIPDATE'].dt.year)['REVENUE'].sum().reset_index()
final_df.columns = ['YEAR', 'REVENUE']

# Write output to CSV
final_df.to_csv('query_output.csv', index=False)
