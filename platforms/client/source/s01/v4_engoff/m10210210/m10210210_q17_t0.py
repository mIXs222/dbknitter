import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query MySQL
query_lineitem = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_QUANTITY
FROM
    lineitem
"""
lineitem_df = pd.read_sql(query_lineitem, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get part DataFrame from Redis
part_df = pd.read_json(redis_connection.get('part'))

# Filter part DataFrame for brand "Brand#23" and container "MED BAG"
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Join DataFrames on partkey
joined_df = lineitem_df.merge(filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average quantity of the filtered parts
average_quantity = joined_df['L_QUANTITY'].mean()

# Calculate average yearly gross loss revenue if orders less than 20% of the average quantity were not taken
threshold_quantity = 0.2 * average_quantity
potential_loss_df = joined_df[joined_df['L_QUANTITY'] < threshold_quantity]
potential_loss_df['YEARLY_LOSS'] = potential_loss_df['L_EXTENDEDPRICE']

# Calculate the average yearly revenue loss
average_loss = potential_loss_df['YEARLY_LOSS'].sum() / 7

# Write the result to a CSV file
output_df = pd.DataFrame({'Average_Yearly_Revenue_Loss': [average_loss]})
output_df.to_csv('query_output.csv', index=False)
