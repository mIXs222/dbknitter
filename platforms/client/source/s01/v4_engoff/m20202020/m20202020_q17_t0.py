# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
try:
    # Read the lineitem table from MySQL
    lineitem_query = "SELECT * FROM lineitem"
    lineitems = pd.read_sql(lineitem_query, conn)
finally:
    conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read the part table from Redis and convert it to a Pandas DataFrame
part_pandas_str = redis_client.get('part')
part_pandas_bytes = bytes(part_pandas_str, encoding='utf8')
part_df = pd.read_json(part_pandas_bytes)

# Filter parts that satisfy the condition: brand 23 and MED BAG
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Join the filtered parts with lineitems on the part key
merged_df = pd.merge(filtered_parts, lineitems, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate the average quantity for the filtered parts
avg_quantity = merged_df['L_QUANTITY'].mean()

# Calculate 20% of the average quantity
quantity_threshold = avg_quantity * 0.2

# Select orders with a quantity of less than the threshold
small_orders = merged_df[merged_df['L_QUANTITY'] < quantity_threshold]

# Calculate the lost yearly revenue by summing up the extended price for the small orders
small_orders['year'] = pd.DatetimeIndex(small_orders['L_SHIPDATE']).year
yearly_loss_revenue = small_orders.groupby('year')['L_EXTENDEDPRICE'].sum().reset_index()

# Calculate average loss revenue per year
avg_yearly_loss_revenue = yearly_loss_revenue['L_EXTENDEDPRICE'].mean()

# Write the result to a CSV file
result = pd.DataFrame({'Average Yearly Gross Loss Revenue': [avg_yearly_loss_revenue]})
result.to_csv('query_output.csv', index=False)
