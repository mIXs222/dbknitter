import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Function to connect to MySQL
def mysql_query():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE
            FROM part
            WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
               OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
               OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
            """
            cursor.execute(sql)
            result = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(result, columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER', 'P_SIZE'])

# Function to connect to Redis
def redis_query():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    df_lineitem = pd.DataFrame(eval(redis_client.get('lineitem')), columns=[
                   'L_ORDERKEY', 'L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPMODE', 'L_SHIPINSTRUCT'])
    return df_lineitem

# Perform the queries to both databases
df_parts = mysql_query()
df_lineitem = redis_query()

# Merge the dataframes on Part Key
df_merged = pd.merge(df_lineitem, df_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter lineitem data based on conditions
df_result = df_merged[
    ((df_merged['P_BRAND'] == 'Brand#12') & (df_merged['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (df_merged['P_SIZE'].between(1, 5)) & 
     (df_merged['L_QUANTITY'].between(1, 11)) & (df_merged['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df_merged['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
    |
    ((df_merged['P_BRAND'] == 'Brand#23') & (df_merged['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (df_merged['P_SIZE'].between(1, 10)) & 
     (df_merged['L_QUANTITY'].between(10, 20)) & (df_merged['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df_merged['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
    |
    ((df_merged['P_BRAND'] == 'Brand#34') & (df_merged['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (df_merged['P_SIZE'].between(1, 15)) & 
     (df_merged['L_QUANTITY'].between(20, 30)) & (df_merged['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (df_merged['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

# Calculate the revenue
df_result['REVENUE'] = df_result['L_EXTENDEDPRICE'] * (1 - df_result['L_DISCOUNT'])

# Group by and sum revenue
df_revenue = df_result.groupby(by=[]).agg({'REVENUE': 'sum'}).reset_index()

# Write the result to a CSV file
df_revenue.to_csv('query_output.csv', index=False)
