import pandas as pd
import pymysql
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to fetch lineitem data from MySQL
mysql_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE 
FROM lineitem 
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve all keys for 'part' data from Redis
part_keys = redis_conn.keys('part:*')
part_data = []
for key in part_keys:
    part_record = redis_conn.hgetall(key)
    # Convert bytes to string in part record
    part_data.append({k.decode('utf-8'): v.decode('utf-8') for k, v in part_record.items()})

# Convert list of dictionaries to DataFrame
part_df = pd.DataFrame(part_data)

# Rename columns to match the MySQL 'lineitem' table
part_df.rename(columns={
    'P_PARTKEY': 'L_PARTKEY',
    'P_RETAILPRICE': 'L_RETAILPRICE'
}, inplace=True)

# Merge data on part key
merged_df = lineitem_df.merge(part_df, how='inner', on='L_PARTKEY')

# Revenue is defined as (l_extendedprice * (1-l_discount))
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'].astype(float))

# Calculate promotion revenue and total revenue
promotion_revenue = merged_df[merged_df['L_RETAILPRICE'].notna()]['REVENUE'].sum()
total_revenue = merged_df['REVENUE'].sum()

# Calculate percentage
promotion_percentage = (promotion_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Create the output DataFrame
output_df = pd.DataFrame({'PROMOTION_PERCENTAGE': [promotion_percentage]})

# Write the output to a CSV file
output_df.to_csv('query_output.csv', index=False)
