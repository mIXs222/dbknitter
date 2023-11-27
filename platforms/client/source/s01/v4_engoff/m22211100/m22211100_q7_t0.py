import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Function to execute a MySQL query and return a DataFrame
def execute_mysql_query(sql):
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(list(result), columns=columns)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch data from MongoDB collections
supplier_df = pd.DataFrame(list(mongodb_db.supplier.find()))
customer_df = pd.DataFrame(list(mongodb_db.customer.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
nation_df = pd.read_json(redis_client.get('nation'), orient='records')

# Define the query for MySQL
mysql_query = """
SELECT
    o.O_CUSTKEY,
    l.L_SUPPKEY,
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) as revenue,
    EXTRACT(YEAR FROM l.L_SHIPDATE) as year
FROM
    orders o
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

# Execute the query and fetch the result
lineitem_orders_df = execute_mysql_query(mysql_query)

# Close the MySQL connection
mysql_conn.close()

# Merge the data frames to get the desired output
merged_df = (
    lineitem_orders_df
    .merge(supplier_df[['S_SUPPKEY', 'S_NATIONKEY']], left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(customer_df[['C_CUSTKEY', 'C_NATIONKEY']], left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df[['N_NATIONKEY', 'N_NAME']], left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(nation_df[['N_NATIONKEY', 'N_NAME']], left_on='C_NATIONKEY', right_on='N_NATIONKEY',
           suffixes=('_SUPPLIER', '_CUSTOMER'))
)

# Filter for suppliers and customers from INDIA and JAPAN
filtered_df = merged_df[
    ((merged_df['N_NAME_SUPPLIER'] == 'INDIA') & (merged_df['N_NAME_CUSTOMER'] == 'JAPAN')) |
    ((merged_df['N_NAME_SUPPLIER'] == 'JAPAN') & (merged_df['N_NAME_CUSTOMER'] == 'INDIA'))
]

# Group by supplier nation, customer nation, and year, and calculate revenue
final_df = (
    filtered_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year'])
    .agg({'revenue':'sum'})
    .reset_index()
    .sort_values(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year'])
)

# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)
