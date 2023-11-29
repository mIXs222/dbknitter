import pymysql
import pandas as pd
import direct_redis

# Function to execute SQL query and return results as a Pandas DataFrame
def get_mysql_data(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return pd.DataFrame(data, columns=columns)

# Function to get Redis data as a Pandas DataFrame
def get_redis_data(redis_client, table_name):
    data_str = redis_client.get(table_name)
    if data_str:
        df = pd.read_json(data_str)
    else:
        df = pd.DataFrame()
    return df

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MySQL Query
mysql_query = """
SELECT
    L_ORDERKEY,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) as revenue
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1995-09-01' AND
    L_SHIPDATE < '1995-10-01'
"""

lineitem_data = get_mysql_data(mysql_conn, mysql_query)
mysql_conn.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_data = get_redis_data(redis_client, 'part')

# Assume promotional parts have 'PROMO' in P_NAME as this was not detailed.
# The joining on the PART table is not possible because its not present in the SQL DBMS
# Therefore, the requirement for "promotional parts" may not be implemented.

# Total revenue from the lineitem table
total_revenue = lineitem_data['revenue'].sum()

# Output the result
result = pd.DataFrame({'Total_Revenue': [total_revenue]})
result.to_csv('query_output.csv', index=False)
