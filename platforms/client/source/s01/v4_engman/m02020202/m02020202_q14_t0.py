# promotion_effect.py
import pymysql.cursors
import pandas as pd
from direct_redis import DirectRedis

# Connecting to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Execute the query on MySQL
query_mysql = """
    SELECT P_PARTKEY, P_NAME FROM part;
"""
mysql_df = pd.read_sql(query_mysql, mysql_connection)
mysql_connection.close()

# Connecting to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)
lineitem_df_str = redis_connection.get('lineitem')
lineitem_df = pd.read_json(lineitem_df_str)

# Filtering lineitem data for the given date range
start_date = '1995-09-01'
end_date = '1995-10-01'
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculating revenue
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Merging tables to consider only parts actually shipped
merged_df = filtered_lineitem_df.merge(mysql_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculating total revenue from filtered lineitem table
total_revenue = merged_df['REVENUE'].sum()

# Identifying promotional parts and calculating promotional revenue
promotional_parts = []  # Need clarification on how to identify promotional parts
promotional_revenue = merged_df[merged_df['P_PARTKEY'].isin(promotional_parts)]['REVENUE'].sum()

# Calculating promotional revenue percentage
promotional_revenue_percentage = (promotional_revenue / total_revenue) * 100.0 if total_revenue else 0

# Output the results to query_output.csv
with open('query_output.csv', 'w') as f:
    f.write('Promotional Revenue Percentage,{}\n'.format(promotional_revenue_percentage))

print("Promotional Revenue Percentage:", promotional_revenue_percentage)
