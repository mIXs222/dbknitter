# Import Libraries
import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MySQL Connection and Query
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
query_mysql = """
SELECT P_PARTKEY, P_TYPE
FROM part
WHERE P_TYPE LIKE 'PROMO%%'
"""
part_df = pd.read_sql(query_mysql, mysql_conn)
mysql_conn.close()

# Redis Connection and Query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_str = redis_conn.get('lineitem')
lineitem_df = pd.read_json(lineitem_str)

# Filter lineitem DataFrame by dates
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-09-30', '%Y-%m-%d')
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Merging to have only promo parts
promo_parts_lineitem_df = pd.merge(filtered_lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the promotional and total revenue
promo_parts_lineitem_df['ADJUSTED_PRICE'] = promo_parts_lineitem_df['L_EXTENDEDPRICE'] * (1 - promo_parts_lineitem_df['L_DISCOUNT'])

# Calculate the metrics
promo_revenue = promo_parts_lineitem_df['ADJUSTED_PRICE'].sum()
total_revenue = filtered_lineitem_df['L_EXTENDEDPRICE'].sum()

# Promo revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Save the output
output_df = pd.DataFrame({'Promo_Revenue_Percentage': [promo_revenue_percentage]})
output_df.to_csv('query_output.csv', index=False)
