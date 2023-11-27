import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   cursorclass=pymysql.cursors.Cursor)
try:
    with mysql_connection.cursor() as cursor:
        # Extract parts
        part_query = """
        SELECT P_PARTKEY, P_NAME 
        FROM part 
        WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
        """
        cursor.execute(part_query)
        parts = cursor.fetchall()
        df_parts = pd.DataFrame(parts, columns=['P_PARTKEY', 'P_NAME'])

finally:
    mysql_connection.close()

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Extract lineitems corresponding to parts and perform the analysis
lineitem_df = pd.read_json(redis_connection.get('lineitem'), orient='records')
lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(df_parts['P_PARTKEY'])]
avg_quantity = lineitem_df['L_QUANTITY'].mean()
small_quantity_threshold = 0.2 * avg_quantity
small_quantity_orders = lineitem_df[lineitem_df['L_QUANTITY'] < small_quantity_threshold]

# Calculate average yearly loss
small_quantity_revenue = small_quantity_orders['L_EXTENDEDPRICE'].sum()
num_years = 7  # considering a 7-year database
average_yearly_loss = small_quantity_revenue / num_years

# Output results to CSV
output_df = pd.DataFrame({'AverageYearlyLoss': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
