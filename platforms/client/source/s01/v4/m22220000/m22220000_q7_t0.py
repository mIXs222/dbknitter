import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime


# Function to execute a SQL query and return a DataFrame
def execute_mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return pd.DataFrame(list(data))

# Convert a Redis key to a DataFrame
def redis_to_df(redis_client, key):
    data = redis_client.get(key)
    df = pd.read_json(data)
    return df

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables into DataFrames
nation_df = redis_to_df(redis_client, 'nation')
supplier_df = redis_to_df(redis_client, 'supplier')

# Load MySQL tables into DataFrames
customer_query = "SELECT * FROM customer"
orders_query = "SELECT * FROM orders"
lineitem_query = "SELECT * FROM lineitem"

customer_df = execute_mysql_query(mysql_conn, customer_query)
orders_df = execute_mysql_query(mysql_conn, orders_query)
lineitem_df = execute_mysql_query(mysql_conn, lineitem_query)

# Close the MySQL connection
mysql_conn.close()

# Convert L_SHIPDATE to datetime and filter rows between '1995-01-01' and '1996-12-31'
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df[3])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) & (lineitem_df['L_SHIPDATE'] <= datetime(1996, 12, 31))]

# Join the DataFrames
merged_df = supplier_df.merge(lineitem_df, left_on=0, right_on=2)
merged_df = merged_df.merge(orders_df, left_on=3, right_on=0)
merged_df = merged_df.merge(customer_df, left_on=4, right_on=0)
merged_df = merged_df.merge(nation_df, left_on=3, right_on=0, suffixes=('_SUPPLIER', '_CUSTOMER'))
merged_df = merged_df[(merged_df['N_NAME_SUPPLIER'].isin(['JAPAN', 'INDIA'])) & (merged_df['N_NAME_CUSTOMER'].isin(['JAPAN', 'INDIA']))]
merged_df['L_YEAR'] = merged_df['L_SHIPDATE'].dt.year

# Calculate VOLUME
merged_df['VOLUME'] = merged_df[10] * (1 - merged_df[11])

# Group by SUPP_NATION, CUST_NATION, L_YEAR
grouped_df = merged_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'L_YEAR']).agg({'VOLUME': 'sum'}).reset_index()
grouped_df = grouped_df.rename(columns={'N_NAME_SUPPLIER': 'SUPP_NATION', 'N_NAME_CUSTOMER': 'CUST_NATION', 'L_YEAR': 'L_YEAR', 'VOLUME': 'REVENUE'})

# Ordering the result
grouped_df = grouped_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Output the result to a CSV file
grouped_df.to_csv('query_output.csv', index=False)
