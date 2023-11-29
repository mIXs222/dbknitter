# query.py
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

# Run query to get parts from MySQL
part_query = """
SELECT P_PARTKEY FROM part
WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG';
"""
parts_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Filter part keys for relevant parts
relevant_part_keys = parts_df['P_PARTKEY'].tolist()

# Connect to Redis and get lineitem data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter lineitem to only have relevant parts
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(relevant_part_keys)]

# Calculate average lineitem quantity for these parts
average_qty = filtered_lineitem_df['L_QUANTITY'].mean()

# Identify the orders which are less than 20% of average quantity
small_qty_orders = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < 0.2 * average_qty]

# Calculate the average yearly gross loss
small_qty_orders['GROSS_LOSS'] = small_qty_orders['L_QUANTITY'] * small_qty_orders['L_EXTENDEDPRICE']
total_loss = small_qty_orders['GROSS_LOSS'].sum()
average_yearly_loss = total_loss / 7  # assuming we have 7 years of data

# Create a DataFrame for the output
output_df = pd.DataFrame({
    'Average Yearly Loss': [average_yearly_loss]
})

# Write the DataFrame to CSV
output_df.to_csv('query_output.csv', index=False)
