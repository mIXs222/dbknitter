import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get the 'part' table from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
"""
part_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get the 'lineitem' table from Redis and convert to pandas DataFrame
lineitem_df = pd.DataFrame(eval(redis_client.get('lineitem')))

# Calculate the average quantity for parts
avg_quantity_df = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity_df.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Merge part dataframe with lineitem dataframe
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Merge with the average quantity dataframe
merged_df = pd.merge(merged_df, avg_quantity_df, how='inner', on='L_PARTKEY')

# Filter line items where the quantity is less than 20% of the average quantity of the same part
filtered_df = merged_df[merged_df['L_QUANTITY'] < 0.2 * merged_df['AVG_QUANTITY']]

# Calculate the sum of extended prices of these line items
sum_extended_price = filtered_df['L_EXTENDEDPRICE'].sum()

# Calculate the average yearly extended price
average_yearly_extended_price = sum_extended_price / 7.0

# Output result to a CSV file
output_df = pd.DataFrame({'Average Yearly Extended Price': [average_yearly_extended_price]})
output_df.to_csv('query_output.csv', index=False)
