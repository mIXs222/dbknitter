import pandas as pd
import pymysql
import direct_redis
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieve part table from MySQL
mysql_query = "SELECT * FROM part"
part_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to the Redis database
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem as Pandas DataFrame from Redis
lineitem_df = redis_conn.get('lineitem')
lineitem_df[['L_EXTENDEDPRICE', 'L_DISCOUNT']] = lineitem_df[['L_EXTENDEDPRICE', 'L_DISCOUNT']].apply(pd.to_numeric)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitem records based on the date range
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1995-09-01')
    & (lineitem_df['L_SHIPDATE'] <= '1995-10-01')
]

# Calculate revenue for each line
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Merge part and lineitem dataframes
result_df = pd.merge(
    filtered_lineitem_df, part_df,
    left_on='L_PARTKEY', right_on='P_PARTKEY'
)

# Calculate total revenue and promotion revenue
total_revenue = result_df['REVENUE'].sum()
promotion_revenue = result_df[result_df['P_CONTAINER'] == 'PROMO']['REVENUE'].sum()

# Calculate and print the percentage of promotional revenue
promotion_percentage = (promotion_revenue / total_revenue) * 100

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMOTION_REVENUE_PERCENTAGE'])
    writer.writerow([promotion_percentage])
