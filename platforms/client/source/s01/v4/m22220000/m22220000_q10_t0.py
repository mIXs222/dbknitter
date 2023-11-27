# Filename: execute_query.py

import pymysql
import pandas as pd
import redis
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             db='tpch', 
                             cursorclass=pymysql.cursors.Cursor)

try:
    # Prepare the MySQL query without the nation table
    mysql_query = """
    SELECT
        customer.C_CUSTKEY,
        customer.C_NAME,
        SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE,
        customer.C_ACCTBAL,
        customer.C_ADDRESS,
        customer.C_PHONE,
        customer.C_COMMENT
    FROM
        customer,
        orders,
        lineitem
    WHERE
        customer.C_CUSTKEY = orders.O_CUSTKEY
        AND lineitem.L_ORDERKEY = orders.O_ORDERKEY
        AND orders.O_ORDERDATE >= '1993-10-01'
        AND orders.O_ORDERDATE < '1994-01-01'
        AND lineitem.L_RETURNFLAG = 'R'
    GROUP BY
        customer.C_CUSTKEY,
        customer.C_NAME,
        customer.C_ACCTBAL,
        customer.C_PHONE,
        customer.C_ADDRESS,
        customer.C_COMMENT
    """

    # Execute MySQL query and store the results in a DataFrame
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_results = cursor.fetchall()
        mysql_df = pd.DataFrame(mysql_results, columns=[
            'C_CUSTKEY',
            'C_NAME',
            'REVENUE',
            'C_ACCTBAL',
            'C_ADDRESS',
            'C_PHONE',
            'C_COMMENT'
        ])

finally:
    mysql_conn.close()

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the nation data from Redis
nation_df = pd.read_json(redis_client.get('nation'))

# Merging the DataFrames on C_NATIONKEY and N_NATIONKEY
merged_df = pd.merge(
    left=mysql_df,
    right=nation_df,
    left_on='C_CUSTKEY',
    right_on='N_NATIONKEY'
)

# Rearrange the merged dataframe to match the desired output columns
final_df = merged_df[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]]

# Order the results
final_df = final_df.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[False, True, True, False]
)

# Write the output to a csv file
final_df.to_csv('query_output.csv', index=False)
