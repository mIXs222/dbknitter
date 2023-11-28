import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from mysql
def get_mysql_data(query, connection):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return pd.DataFrame(data)

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to get parts data from MySQL
mysql_query = """
SELECT * FROM part WHERE
(P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')) OR
(P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')) OR
(P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))
"""

# Run the query and get a DataFrame
part_df = get_mysql_data(mysql_query, mysql_connection)

# Connect to Redis database 0
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_df = pd.read_json(redis_connection.get('lineitem'), orient='records')

# Close the MySQL connection
mysql_connection.close()

# Apply conditions to lineitem DataFrame
conditions = (
    ((lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') & (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11)) |
      ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20)) |
      ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30))))
)

filtered_lineitem_df = lineitem_df[conditions]

# Merge the parts and lineitem DataFrames on P_PARTKEY == L_PARTKEY
merged_df = pd.merge(part_df, filtered_lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Apply the size conditions
size_conditions = (
    (merged_df['P_BRAND'] == 'Brand#12' & merged_df['P_SIZE'].between(1, 5)) |
    (merged_df['P_BRAND'] == 'Brand#23' & merged_df['P_SIZE'].between(1, 10)) |
    (merged_df['P_BRAND'] == 'Brand#34' & merged_df['P_SIZE'].between(1, 15))
)

final_df = merged_df[size_conditions]

# Calculate the revenue
final_df['revenue'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Group by the required fields and sum the revenue
output_df = final_df.groupby(['L_ORDERKEY', 'P_BRAND', 'P_CONTAINER']).agg({'revenue': 'sum'}).reset_index()

# Write the output to a CSV file
output_df.to_csv('query_output.csv', index=False)
