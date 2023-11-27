# File: query_execution.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

with connection.cursor() as cursor:
    # Perform the SQL query only on tables within MySQL
    mysql_query = """
    SELECT
        YEAR(O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
        O_ORDERKEY, C_CUSTKEY, C_NATIONKEY
    FROM
        lineitem
    JOIN orders ON L_ORDERKEY = O_ORDERKEY
    JOIN customer ON O_CUSTKEY = C_CUSTKEY
    WHERE
        O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    cursor.execute(mysql_query)
    mysql_data = cursor.fetchall()

# Convert MySQL data to pandas DataFrame
mysql_columns = ['O_YEAR', 'VOLUME', 'O_ORDERKEY', 'C_CUSTKEY', 'C_NATIONKEY']
mysql_df = pd.DataFrame(mysql_data, columns=mysql_columns)

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)
# Retrieve Redis databases as pandas DataFrames
nation_df = pd.read_json(redis.get('nation'))
region_df = pd.read_json(redis.get('region'))
part_df = pd.read_json(redis.get('part'))
supplier_df = pd.read_json(redis.get('supplier'))

# Filter part and supplier tables for the query conditions
part_df = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Merge Redis and MySQL data
merged_df = mysql_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df[region_df['R_NAME'] == 'ASIA'], how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Perform the group by operations as per the SQL query
result = merged_df.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': (x[x['N_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum())
    })
).reset_index()

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False)
