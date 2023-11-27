# python code to execute the query
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import direct_redis

# Establish a connection to the MySQL database
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Retrieve data from MySQL tables
supplier_query = """
SELECT 
    S_SUPPKEY, S_NATIONKEY
FROM
    supplier
"""
customer_query = """
SELECT 
    C_CUSTKEY, C_NATIONKEY
FROM
    customer
"""
lineitem_query = """
SELECT 
    L_SUPPKEY, L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM
    lineitem
WHERE 
    L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(supplier_query)
    supplier_data = cursor.fetchall()
    df_supplier = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NATIONKEY'])

    cursor.execute(customer_query)
    customer_data = cursor.fetchall()
    df_customer = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NATIONKEY'])

    cursor.execute(lineitem_query)
    lineitem_data = cursor.fetchall()
    df_lineitem = pd.DataFrame(lineitem_data, columns=['L_SUPPKEY', 'L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE'])

# Close the MySQL connection
mysql_connection.close()

# Establish a connection to the Redis database
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis tables
df_nation = pd.read_json(r.get('nation'))
df_orders = pd.read_json(r.get('orders'))

# Filter nations for INDIA and JAPAN
nation_filter = df_nation['N_NAME'].isin(['INDIA', 'JAPAN'])
df_nation = df_nation[nation_filter]

# Compute gross discounted revenues
# Merge lineitems with supplier's nation
df_lineitem = df_lineitem.merge(df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Merge lineitems with customer's nation
df_lineitem = df_lineitem.merge(df_customer, left_on='L_ORDERKEY', right_on='C_CUSTKEY')

# Merge orders information
df_lineitem = df_lineitem.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculate the revenue
df_lineitem['YEAR'] = pd.to_datetime(df_lineitem['L_SHIPDATE']).dt.year
df_lineitem['REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])

# Filter based on the condition (supplier nation to customer nation)
filt1 = (df_lineitem['S_NATIONKEY'] == df_nation.loc[df_nation['N_NAME'] == "INDIA", 'N_NATIONKEY'].iloc[0]) \
        & (df_lineitem['C_NATIONKEY'] == df_nation.loc[df_nation['N_NAME'] == "JAPAN", 'N_NATIONKEY'].iloc[0])
filt2 = (df_lineitem['S_NATIONKEY'] == df_nation.loc[df_nation['N_NAME'] == "JAPAN", 'N_NATIONKEY'].iloc[0]) \
        & (df_lineitem['C_NATIONKEY'] == df_nation.loc[df_nation['N_NAME'] == "INDIA", 'N_NATIONKEY'].iloc[0])

df_result = df_lineitem[filt1 | filt2]

# Group by and aggregate
output = df_result.groupby(['S_NATIONKEY', 'C_NATIONKEY', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

# Mapping the keys back to nation names
nation_key_map = df_nation.set_index('N_NATIONKEY')['N_NAME'].to_dict()
output['SUPPLIER_NATION'] = output['S_NATIONKEY'].map(nation_key_map)
output['CUSTOMER_NATION'] = output['C_NATIONKEY'].map(nation_key_map)

# Selecting and renaming columns
output = output[['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR', 'REVENUE']]

# Sorting the result
output = output.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR'])

# Saving to csv
output.to_csv('query_output.csv', index=False)
