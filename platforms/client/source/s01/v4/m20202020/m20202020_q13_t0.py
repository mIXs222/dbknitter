# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
            FROM customer
            """
        )
        customer_records = cursor.fetchall()
        df_customers = pd.DataFrame(customer_records, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_data = redis_conn.get('orders')
df_orders = pd.read_json(orders_data)

# Prepare dataframe according to the given SQL JOIN query
df_orders_filtered = df_orders[df_orders['O_COMMENT'].str.contains('pending%deposits%', regex=False)]

# Perform left join
df_merged = pd.merge(df_customers, df_orders_filtered, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Group by C_CUSTKEY and count O_ORDERKEY
grouped = df_merged.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Further group by C_COUNT and count occurrences
custdist = grouped.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

# Sort the result
custdist_sorted = custdist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Output to CSV
custdist_sorted.to_csv('query_output.csv', index=False)
