import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_connection = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch",
)

# Read tables from tpch in MySQL (customer and lineitem)
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM customer;")
    customers_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    cursor.execute("""SELECT L_ORDERKEY, SUM(L_QUANTITY) as sum_quantity
                        FROM lineitem
                        GROUP BY L_ORDERKEY
                        HAVING SUM(L_QUANTITY) > 300;""")
    lineitem_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Close MySQL connection
mysql_connection.close()

# Redis connection
redis_connection = DirectRedis(host="redis", port=6379, db=0)

# Read table from Redis (orders)
orders_df = pd.read_json(redis_connection.get('orders'), orient='records')

# Filter orders based on lineitem data
filtered_orders = orders_df[orders_df['O_ORDERKEY'].isin(lineitem_df['L_ORDERKEY'])]

# Merge filtered_orders with customers
results = pd.merge(
    customers_df,
    filtered_orders,
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
)

# Select required columns
required_columns = [
    'C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'sum_quantity'
]

results = results[required_columns]

# Sort by price descending and then by date
results.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Saving to CSV
results.to_csv('query_output.csv', index=False)
