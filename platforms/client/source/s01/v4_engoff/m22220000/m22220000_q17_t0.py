import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to get average quantity of parts with P_BRAND = 'Brand#23' and P_CONTAINER = 'MED BAG'
part_query = "SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
# Use DirectRedis to get the 'part' DataFrame
redis = DirectRedis(host='redis', port=6379, db=0)
part_df = redis.get('part')
part_df = pd.read_json(part_df)

# Filter parts from the MySQL 'lineitem' table and calculate the average quantity
# for parts from the 'part' DataFrame
average_quantity_query = """
SELECT AVG(L_QUANTITY) AS average_quantity FROM lineitem
WHERE L_PARTKEY IN ({})
"""
qualified_parts = ','.join(map(str, part_df[part_df['P_BRAND']=='Brand#23'][part_df['P_CONTAINER']=='MED BAG']['P_PARTKEY'].tolist()))
average_quantity_query = average_quantity_query.format(qualified_parts)

with mysql_conn.cursor() as cursor:
    cursor.execute(average_quantity_query)
    average_quantity = cursor.fetchone()[0]

# Calculate the average yearly gross loss for lineitems with quantity less than 20% of the average
gross_loss_query = f"""
SELECT SUM(L_EXTENDEDPRICE) / 7 AS average_yearly_loss FROM lineitem
WHERE L_QUANTITY < {0.2 * average_quantity}
AND L_PARTKEY IN ({qualified_parts})
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(gross_loss_query)
    average_yearly_loss = cursor.fetchone()[0]

# Close MySQL connection
mysql_conn.close()

# Write the result to a CSV file
df_result = pd.DataFrame({'average_yearly_loss': [average_yearly_loss]})
df_result.to_csv('query_output.csv', index=False)
