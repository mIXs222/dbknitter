# query_script.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish connection to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4')

try:
    # Connect to mysql and get relevant parts
    with mysql_conn.cursor() as cursor:
        part_query = """
            SELECT P_PARTKEY, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE
            FROM part
            WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
        """
        cursor.execute(part_query)
        parts_data = cursor.fetchall()

    # Create the Parts DataFrame
    columns = ['P_PARTKEY', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE']
    parts_df = pd.DataFrame(list(parts_data), columns=columns)

finally:
    mysql_conn.close()

# Establish connection to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis and create DataFrame
lineitem_data = redis_conn.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filter lineitem for relevant parts
relevant_part_keys = parts_df['P_PARTKEY'].unique()
filtered_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(relevant_part_keys)]

# Calculate the average quantity of such parts ordered
avg_quantity = filtered_lineitem_df['L_QUANTITY'].mean()

# Determine orders less than 20% of the average quantity
small_quantity_orders = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < (0.2 * avg_quantity)]

# Calculate the average yearly gross loss in revenue
small_quantity_orders['YEARLY_LOSS'] = small_quantity_orders['L_EXTENDEDPRICE']
avg_yearly_gross_loss = small_quantity_orders['YEARLY_LOSS'].sum() / 7

# Write result to a CSV file
result = pd.DataFrame([{'avg_yearly_gross_loss': avg_yearly_gross_loss}])
result.to_csv('query_output.csv', index=False)
