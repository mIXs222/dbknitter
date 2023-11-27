import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   cursorclass=pymysql.cursors.Cursor)
try:
    with mysql_connection.cursor() as cursor:
        # Define the MySQL query excluding the customer table
        mysql_query = """
        SELECT
            L_ORDERKEY,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
            O_ORDERDATE,
            O_SHIPPRIORITY
        FROM
            orders,
            lineitem
        WHERE
            C_CUSTKEY = O_CUSTKEY
            AND L_ORDERKEY = O_ORDERKEY
            AND O_ORDERDATE < '1995-03-15'
            AND L_SHIPDATE > '1995-03-15'
        GROUP BY
            L_ORDERKEY,
            O_ORDERDATE,
            O_SHIPPRIORITY
        """
        # Execute the query and fetch the result
        cursor.execute(mysql_query)
        mysql_result = cursor.fetchall()

        # Convert the result to a DataFrame
        mysql_df = pd.DataFrame(mysql_result, columns=['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
finally:
    mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
redis_df = pd.DataFrame.from_dict(redis_connection.get('customer'))

# Filter redis_df with C_MKTSEGMENT column
filtered_redis_df = redis_df[redis_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge the dataframes
result_df = pd.merge(filtered_redis_df, mysql_df, left_on='C_CUSTKEY', right_on='L_ORDERKEY')

# Sort and output the result to a csv file
result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)
result_df.to_csv('query_output.csv', index=False)
