import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to calculate the promotional revenue
def calculate_promo_revenue(lineitem_df, part_pkeys):
    lineitem_promo = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_pkeys)]
    promo_revenue = (lineitem_promo['L_EXTENDEDPRICE'] * (1 - lineitem_promo['L_DISCOUNT'])).sum()
    total_revenue = (lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])).sum()
    return (promo_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

# Load 'lineitem' table where L_SHIPDATE is between given dates
query = """
    SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1995-09-01' AND '1995-10-01';
"""
lineitem = pd.read_sql(query, mysql_conn)
mysql_conn.close()

# Connect to Redis and get the 'part' table
r_conn = DirectRedis(host='redis', port=6379, db=0)
part_keys_encoded = r_conn.get('part')
part_keys_str = part_keys_encoded.decode('utf-8')
part_pkeys = set([int(x) for x in part_keys_str.split(',') if x.isdigit()])

# Calculate promotional revenue percentage
promo_revenue_percent = calculate_promo_revenue(lineitem, part_pkeys)

# Write output to CSV
pd.DataFrame({'Promotion_Revenue_Percentage': [promo_revenue_percent]}).to_csv('query_output.csv', index=False)
