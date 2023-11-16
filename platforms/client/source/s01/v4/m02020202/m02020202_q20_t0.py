# query.py

import pymysql
import pandas as pd
import direct_redis

# Function to connect to MySQL and execute a query
def execute_mysql_query(query):
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    try:
        df = pd.read_sql_query(query, connection)
    finally:
        connection.close()
    return df

# Function to connect to Redis and execute a query
def execute_redis_query(table_name):
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    data = r.get(table_name)
    if data is not None:
        df = pd.read_json(data, orient='records')
        return df
    return pd.DataFrame()

# Get relevant data from MySQL
subquery_part = """SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"""
subquery_partsupp = f"""
    SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY IN ({subquery_part})
"""
subquery_nation = """SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'"""
df_partsupp = execute_mysql_query(subquery_partsupp)
df_nation = execute_mysql_query(subquery_nation)

# Get relevant data from Redis
df_lineitem = execute_redis_query('lineitem')

# Further filtering based on the subqueries
df_lineitem_filtered = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= '1994-01-01') &
    (df_lineitem['L_SHIPDATE'] < '1995-01-01')
]
df_needed_partsupp = df_partsupp[df_partsupp['PS_PARTKEY'].isin(df_lineitem_filtered['L_PARTKEY']) &
                                 df_partsupp['PS_SUPPKEY'].isin(df_lineitem_filtered['L_SUPPKEY'])]

# Continuing the query with MySQL and Redis data combined
df_supplier = execute_redis_query('supplier')
df_supplier_filtered = df_supplier[
    df_supplier['S_SUPPKEY'].isin(df_needed_partsupp['PS_SUPPKEY']) &
    df_supplier['S_NATIONKEY'].isin(df_nation['N_NATIONKEY'])
]

# Final query result
result = df_supplier_filtered[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

# Writing results to a CSV file
result.to_csv('query_output.csv', index=False)
