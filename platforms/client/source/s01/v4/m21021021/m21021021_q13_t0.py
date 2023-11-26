import pymysql
import pandas as pd
import redis
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            query = 'SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT FROM customer;'
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            customer_df = pd.DataFrame(list(result), columns=columns)
    finally:
        connection.close()
    return customer_df

# Function to get data from Redis
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    orders_df = pd.read_json(r.get('orders'), orient='records')
    return orders_df

# Get data from databases
customer_df = get_mysql_data()
orders_df = get_redis_data()

# Merge and perform the query
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left', suffixes=('', '_orders'))
merged_df_filtered = merged_df[~merged_df['O_COMMENT'].str.contains('pending%deposits%', na=False)]
c_orders = merged_df_filtered.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()
custdist_df = c_orders.groupby('C_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'count')).reset_index().sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write the final output to csv
custdist_df.to_csv('query_output.csv', index=False)
