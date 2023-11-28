import pymysql
import pandas as pd
import direct_redis

# Function to calculate revenue
def calculate_revenue(dataframe):
    dataframe['REVENUE'] = dataframe['L_EXTENDEDPRICE'] * (1 - dataframe['L_DISCOUNT'])
    return dataframe['REVENUE'].sum()

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = '''
SELECT *
FROM lineitem
WHERE (L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
'''
lineitem_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve part data from Redis as DataFrame
part_df = pd.read_json(redis_client.get('part').decode('utf-8'))

# Applying the conditions and merging with part_df
merged_df = lineitem_df[lineitem_df['L_QUANTITY'].between(1, 11) & lineitem_df['L_PARTKEY'].isin(part_df[part_df['P_BRAND'] == 'Brand#12']['P_PARTKEY'])]
merged_df = merged_df.append(lineitem_df[lineitem_df['L_QUANTITY'].between(10, 20) & lineitem_df['L_PARTKEY'].isin(part_df[part_df['P_BRAND'] == 'Brand#23']['P_PARTKEY'])], ignore_index=True)
merged_df = merged_df.append(lineitem_df[lineitem_df['L_QUANTITY'].between(20, 30) & lineitem_df['L_PARTKEY'].isin(part_df[part_df['P_BRAND'] == 'Brand#34']['P_PARTKEY'])], ignore_index=True)

# Calculate revenue
total_revenue = calculate_revenue(merged_df)

# Output result to CSV file
results_df = pd.DataFrame({'Total_Revenue': [total_revenue]})
results_df.to_csv('query_output.csv', index=False)
