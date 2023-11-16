import pymysql
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Query to get nation and supplier from MySQL
mysql_query = """
SELECT
    S_SUPPKEY,
    N_NATIONKEY,
    N_NAME
FROM
    supplier,
    nation
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND (
        N_NAME = 'JAPAN'
        OR N_NAME = 'INDIA'
    )
"""

supplier_nation = pd.read_sql(mysql_query, mysql_conn)
supplier_nation.rename(columns={'N_NAME': 'SUPP_NATION', 'N_NATIONKEY': 'S_NATIONKEY'}, inplace=True)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load DataFrames from Redis
customer = pd.read_json(redis_client.get('customer'), orient='records')
orders = pd.read_json(redis_client.get('orders'), orient='records')
lineitem = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter customers from Japan and India
customer = customer[customer['C_NATIONKEY'].isin([supplier_nation['S_NATIONKEY']])]

# Convert date strings to datetime objects for comparison
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
lineitem = lineitem[(lineitem['L_SHIPDATE'] >= datetime.date(1995, 1, 1)) & (lineitem['L_SHIPDATE'] <= datetime.date(1996, 12, 31))]

# Merge tables to simulate joining
merged_df = pd.merge(lineitem, orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, supplier_nation, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter for nation conditions
merged_df = merged_df[((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['C_NATIONKEY'] == 'INDIA')) |
                      ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['C_NATIONKEY'] == 'JAPAN'))]

# Calculate the VOLUME column
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by the required fields
result = merged_df.groupby(['SUPP_NATION', 'C_NATIONKEY', merged_df['L_SHIPDATE'].dt.year]).agg(REVENUE=pd.NamedAgg(column='VOLUME', aggfunc='sum')).reset_index()
result.rename(columns={'C_NATIONKEY': 'CUST_NATION', 'L_SHIPDATE': 'L_YEAR'}, inplace=True)

# Sort by the required fields
result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Save the result to CSV
result.to_csv('query_output.csv', index=False)
