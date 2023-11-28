import pymysql
import direct_redis
import pandas as pd

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Redis connection parameters
redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Establish MySQL connection
mysql_conn = pymysql.connect(**mysql_params)
try:
    with mysql_conn.cursor() as cursor:
        # Execute query to fetch lineitem data within the given shipping date range
        shipping_start = "1995-09-01"
        shipping_end = "1995-09-30"
        cursor.execute("""
        SELECT L_PARTKEY, (L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN %s AND %s
        """, (shipping_start, shipping_end))
        lineitem_data = cursor.fetchall()
finally:
    mysql_conn.close()

# Create DataFrame from fetched lineitem data
lineitem_df = pd.DataFrame(lineitem_data, columns=['P_PARTKEY', 'revenue'])

# Establish Redis connection
r = direct_redis.DirectRedis(**redis_params)

# Fetch part data from Redis
part_data = r.get('part')

# Create DataFrame from fetched part data
part_df = pd.read_json(part_data)

# Merge part and lineitem DataFrames on part key
merged_df = pd.merge(lineitem_df, part_df, on='P_PARTKEY')

# Calculate the total revenue and promotional revenue
total_revenue = merged_df['revenue'].sum()
promo_revenue = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['revenue'].sum()

# Calculate the promotional revenue as a percentage of the total revenue
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Writing results to 'query_output.csv'
result_df = pd.DataFrame({
    'Promotional Revenue (%)': [promo_revenue_percentage]
})
result_df.to_csv('query_output.csv', index = False)
