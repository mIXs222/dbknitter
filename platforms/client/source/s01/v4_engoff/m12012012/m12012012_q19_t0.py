# query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query for MySQL (only parts from the 'part' table)
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""

# Execute MySQL Query
mysql_cursor.execute(mysql_query)
part_results = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER'])
mysql_conn.close()

# Get lineitem DataFrame from Redis
try:
    lineitem_df = redis_conn.get('lineitem')  # Retrieving it as a DataFrame

    # Consider lineitem_df to be a DataFrame with the relevant columns
    # Filter lineitem DataFrame according to the conditions
    filtered_lineitem_df = lineitem_df[
        ((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11) &
         (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
         (lineitem_df['L_PARTKEY'].isin(part_results[part_results['P_SIZE'].between(1, 5)]['P_PARTKEY']))) |
        ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20) &
         (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
         (lineitem_df['L_PARTKEY'].isin(part_results[part_results['P_SIZE'].between(1, 10)]['P_PARTKEY']))) |
        ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30) &
         (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
         (lineitem_df['L_PARTKEY'].isin(part_results[part_results['P_SIZE'].between(1, 15)]['P_PARTKEY'])))
    ]

    # Calculate the gross discounted revenue
    filtered_lineitem_df['DISCOUNT_PRICE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

    # Write results to query_output.csv
    filtered_lineitem_df.to_csv('query_output.csv', index=False)
except Exception as e:
    print(f"An error occurred while processing the lineitem table from Redis: {e}")
