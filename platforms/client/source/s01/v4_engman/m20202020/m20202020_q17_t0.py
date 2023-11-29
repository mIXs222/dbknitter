import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Retrieve lineitem data from MySQL
mysql_query = """
    SELECT L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM lineitem
"""
mysql_cursor.execute(mysql_query)
lineitem_data = pd.DataFrame(mysql_cursor.fetchall(), columns=["L_PARTKEY", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT"])

# Connect to Redis and retrieve part data as a Pandas DataFrame
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_data = pd.read_json(redis_client.get('part'), orient='records')

# Filter part data for BRAND#23 and MED BAG
filtered_parts = part_data[(part_data['P_BRAND'] == 'BRAND#23') & (part_data['P_CONTAINER'] == 'MED BAG')]
filtered_part_keys = filtered_parts['P_PARTKEY'].unique()

# Join lineitem data with filtered part keys
relevant_lineitems = lineitem_data[lineitem_data['L_PARTKEY'].isin(filtered_part_keys)]

# Calculate the average quantity
avg_quantity = relevant_lineitems['L_QUANTITY'].mean()

# Calculate relevant quantity threshold
quantity_threshold = avg_quantity * 0.2

# Determine the gross loss per line for small quantity orders (< 20% average quantity)
relevant_lineitems['LOSS'] = relevant_lineitems.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['L_QUANTITY'] < quantity_threshold else 0,
    axis=1
)

# Calculate average yearly loss
total_years = 7
avg_yearly_loss = relevant_lineitems['LOSS'].sum() / total_years

# Prepare output to csv
output_df = pd.DataFrame({"average_yearly_loss": [avg_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
