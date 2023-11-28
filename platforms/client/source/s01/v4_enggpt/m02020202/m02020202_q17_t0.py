import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
cursor = mysql_conn.cursor()

# Get parts from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
cursor.execute(part_query)
part_columns = [desc[0] for desc in cursor.description]
part_data = cursor.fetchall()
df_parts = pd.DataFrame(part_data, columns=part_columns)
filtered_part_keys = df_parts['P_PARTKEY'].tolist()

# Close MySQL connection
cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Get lineitem from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter lineitem based on part keys from MySQL
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(filtered_part_keys)]

# Calculate average quantity of each part across all line items
avg_qty_per_part = filtered_lineitem_df.groupby("L_PARTKEY")["L_QUANTITY"].mean().reset_index()

# Calculate 20% of the average quantity
avg_qty_per_part["20%_avg_qty"] = avg_qty_per_part["L_QUANTITY"] * 0.2

# Merge to associate 20% average quantity back with the line items
merged_df = pd.merge(filtered_lineitem_df, avg_qty_per_part[["L_PARTKEY", "20%_avg_qty"]],
                     on="L_PARTKEY", how="inner")

# Filter line items where quantity is less than 20% of average
final_lineitem_df = merged_df[merged_df["L_QUANTITY"] < merged_df["20%_avg_qty"]]

# Calculate the yearly average extended price
final_lineitem_df['avg_yearly_ext_price'] = final_lineitem_df['L_EXTENDEDPRICE'].sum() / 7.0

# Generate output and write to CSV file
final_output = final_lineitem_df[["L_ORDERKEY", "avg_yearly_ext_price"]]
final_output.to_csv('query_output.csv', index=False)
