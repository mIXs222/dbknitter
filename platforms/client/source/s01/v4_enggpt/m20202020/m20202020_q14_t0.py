import pymysql
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Function to connect to MySQL database and execute query
def fetch_mysql_data(query):
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.Cursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            col_descriptions = cursor.description
            col_names = [col[0] for col in col_descriptions]
            data_frame = pd.DataFrame(list(result), columns=col_names)
    finally:
        connection.close()
    return data_frame

# Function to connect to Redis and get data 
def fetch_redis_data():
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    part_data = redis_conn.get('part')
    part_df = pd.DataFrame(part_data)
    return part_df

# Fetch data from MySQL for lineitem table
query_lineitem = """
SELECT
    L_ORDERKEY, L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
"""
lineitem_df = fetch_mysql_data(query_lineitem)

# Fetch data from Redis for part table
part_df = fetch_redis_data()

# Merge datasets
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter promotional items
promo_items_df = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]

# Calculate the sums
promo_revenue = (promo_items_df['L_EXTENDEDPRICE'] * (1 - promo_items_df['L_DISCOUNT'])).sum()
total_revenue = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# Calculate promotional revenue as a percentage of total revenue
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Save to CSV
result_df = pd.DataFrame({
    'Promo_Revenue_Percentage': [promo_revenue_percentage]
})
result_df.to_csv('query_output.csv', index=False)
