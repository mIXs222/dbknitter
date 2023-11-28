import pymongo
import pymysql
import pandas as pd
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host = 'mysql',
    user = 'root',
    password = 'my-secret-pw',
    database = 'tpch',
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
import redis
import pickle
redis_conn = redis.StrictRedis(host='redis', port=6379, db=0)

# Query MySQL for orders and supplier
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT s.S_NAME, o.O_ORDERKEY, o.O_ORDERDATE, n.N_NAME as SUPPLIER_NATION
    FROM supplier s
    JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN orders o ON o.O_ORDERKEY = s.S_SUPPKEY
    WHERE n.N_NAME IN ('JAPAN', 'INDIA')
    AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31';
    """)
    supplier_orders = cursor.fetchall()

# Convert MySQL data to DataFrame
df_supplier_orders = pd.DataFrame(supplier_orders, columns=['S_NAME', 'O_ORDERKEY', 'O_ORDERDATE', 'SUPPLIER_NATION'])

# Query MongoDB for customer
customers_cursor = mongodb_db.customer.find(
    {"C_NATIONKEY": {"$in": ["JAPAN", "INDIA"]}},
    {"C_NAME": 1, "C_CUSTKEY": 1, "_id": 0}
)
df_customers = pd.DataFrame(list(customers_cursor))

# Query Redis for lineitem
lineitem_data = redis_conn.get('lineitem')
df_lineitem = pd.DataFrame(pickle.loads(lineitem_data))

# Filter Redis data for timeframe
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
df_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= datetime(1995, 1, 1)) & (df_lineitem['L_SHIPDATE'] <= datetime(1996, 12, 31))]

# Merge data
merged_df = pd.merge(df_supplier_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(df_customers, left_on='O_ORDERKEY', right_on='C_CUSTKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Filter for nations of interest and group by supplier nation, customer nation, and year
result = merged_df[
    ((merged_df['SUPPLIER_NATION'] == 'JAPAN') & (merged_df['C_NATIONKEY'] == 'INDIA')) |
    ((merged_df['SUPPLIER_NATION'] == 'INDIA') & (merged_df['C_NATIONKEY'] == 'JAPAN'))
].groupby(
    [merged_df['SUPPLIER_NATION'], merged_df['C_NATIONKEY'], merged_df['L_SHIPDATE'].dt.year]
).agg(
    {'REVENUE': 'sum'}
).reset_index().rename(columns={'L_SHIPDATE': 'YEAR'})

# Sort the results
result_sorted = result.sort_values(by=['SUPPLIER_NATION', 'C_NATIONKEY', 'YEAR'])

# Write to CSV
result_sorted.to_csv('query_output.csv', index=False)

mysql_conn.close()
mongodb_client.close()
