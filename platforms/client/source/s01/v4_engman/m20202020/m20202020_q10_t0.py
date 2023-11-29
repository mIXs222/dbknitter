# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Read customer and lineitem data from MySQL
with mysql_conn.cursor() as cursor:
    customer_query = "SELECT * FROM customer;"
    cursor.execute(customer_query)
    customers = cursor.fetchall()

    lineitem_query = """
    SELECT
        L_CUSTKEY,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue_lost
    FROM lineitem
    WHERE
        L_SHIPDATE >= '1993-10-01' AND
        L_SHIPDATE < '1994-01-01' AND
        L_RETURNFLAG = 'R'
    GROUP BY L_CUSTKEY;
    """
    cursor.execute(lineitem_query)
    lineitems = cursor.fetchall()

# Convert tuples to DataFrame
customer_columns = ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"]
customers_df = pd.DataFrame(customers, columns=customer_columns)

lineitem_columns = ["L_CUSTKEY", "revenue_lost"]
lineitems_df = pd.DataFrame(lineitems, columns=lineitem_columns)

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Read nation data from Redis
nation_df = pd.read_json(redis_conn.get('nation'))

# Combine data from different platforms
combined_df = pd.merge(customers_df, lineitems_df, how='inner', left_on='C_CUSTKEY', right_on='L_CUSTKEY')
combined_df = pd.merge(combined_df, nation_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and sort the final output
output_df = combined_df[
    ["C_CUSTKEY", "C_NAME", "revenue_lost", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT"]
].sort_values(
    by=["revenue_lost", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"],
    ascending=[True, True, True, False]
)

# Write output to CSV
output_df.to_csv("query_output.csv", index=False)

# Close database connections
mysql_conn.close()
redis_conn.close()
