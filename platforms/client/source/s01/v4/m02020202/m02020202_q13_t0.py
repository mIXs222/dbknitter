import pymysql
import pandas as pd
import direct_redis

# Function to execute a MySQL query and return the result as a DataFrame
def execute_mysql_query(query, connection_info):
    try:
        conn = pymysql.connect(
            host=connection_info["hostname"],
            user=connection_info["username"],
            passwd=connection_info["password"],
            db=connection_info["database name"]
        )
        result_df = pd.read_sql(query, conn)
        conn.close()
        return result_df
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return pd.DataFrame()

# Function to get Redis data and return as a DataFrame
def get_redis_data(key, connection_info):
    try:
        r = direct_redis.DirectRedis(host=connection_info["hostname"], port=connection_info["port"])
        data = r.get(key)
        if data:
            return pd.read_json(data, orient='split')
        else:
            raise ValueError(f"No data found for key '{key}' in Redis.")
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return pd.DataFrame()

mysql_info = {
    "database name": "tpch",
    "username": "root",
    "password": "my-secret-pw",
    "hostname": "mysql"
}

redis_info = {
    "database name": "0",
    "hostname": "redis",
    "port": 6379
}

# Get the data from MySQL
mysql_query = "SELECT O_ORDERKEY, O_CUSTKEY FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%'"
orders_df = execute_mysql_query(mysql_query, mysql_info)

# Get the data from Redis
customer_df = get_redis_data("customer", redis_info)

# Left join the Redis and MySQL data
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Execute the user's intended query
result = merged_df.groupby('C_COUNT').agg(CUSTDIST=('C_COUNT', 'count')).reset_index()
result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write the result to CSV
result.to_csv('query_output.csv', index=False)
