import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to get lineitem data from MySQL
lineitem_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_QUANTITY
FROM lineitem;
"""

# Execute the query and fetch the data into DataFrame
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to the Redis database
redis_conn = DirectRedis(host='redis', port=6379)

# Fetch the 'part' table from Redis and convert the json to DataFrame
part_dict = redis_conn.get('part')
part_df = pd.DataFrame.from_dict(part_dict)

# Filter the part data for the brand and container type
filtered_part_df = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the average quantity of parts
average_quantity_df = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
average_quantity_df.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Join the DataFrame to find less than 20% of the average quantity
filtered_lineitem_df = pd.merge(lineitem_df, average_quantity_df, on='L_PARTKEY', how='inner')
filtered_lineitem_df = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < (0.2 * filtered_lineitem_df['AVG_QUANTITY'])]

# Join with the filtered part DataFrame to get final filtered results on brand and container type
final_df = pd.merge(filtered_lineitem_df, filtered_part_df, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')

# Calculate the total sum of extended prices and divide by 7.0 to get the average yearly extended price
final_df['YEARLY_AVG_EXTENDED_PRICE'] = final_df['L_EXTENDEDPRICE'].sum() / 7.0

# Write the result to the output CSV file
final_df[['YEARLY_AVG_EXTENDED_PRICE']].to_csv('query_output.csv', index=False)
