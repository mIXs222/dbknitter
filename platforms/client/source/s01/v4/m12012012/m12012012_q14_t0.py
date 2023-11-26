import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL
def connect_to_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.Cursor)
    return connection

# Function to fetch data from MySQL
def get_mysql_data(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(result, columns=columns)

# Function to connect to Redis
def connect_to_redis():
    return DirectRedis(host='redis', port=6379, db=0)

# Function to fetch data from Redis as a DataFrame
def get_redis_data(redis_client, key):
    data = redis_client.get(key)
    if data:
        df = pd.read_json(data)
        return df
    else:
        return pd.DataFrame()

# Connect to the databases
mysql_conn = connect_to_mysql()
redis_client = connect_to_redis()

# Query the data
part_sql_query = """
SELECT P_PARTKEY, P_TYPE
FROM part;
"""

part_df = get_mysql_data(mysql_conn, part_sql_query)

lineitem_df = get_redis_data(redis_client, 'lineitem')

# Close MySQL Connection
mysql_conn.close()

# Join and process data
merged_df = part_df.merge(lineitem_df, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

filtered_df = merged_df[
    (merged_df['L_SHIPDATE'] >= '1995-09-01') &
    (merged_df['L_SHIPDATE'] < '1995-10-01')
]

# Calculate PROMO_REVENUE
filtered_df['PROMO_REVENUE'] = filtered_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0,
    axis=1
)

total_revenue = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
promo_revenue = filtered_df['PROMO_REVENUE']

# Ensuring no division by zero
result = (100.00 * promo_revenue.sum() / total_revenue.sum()) if total_revenue.sum() != 0 else 0
result_df = pd.DataFrame({'PROMO_REVENUE': [result]})

# Output result to CSV
result_df.to_csv('query_output.csv', index=False)
