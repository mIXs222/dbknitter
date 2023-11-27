import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query MySQL to get required lineitem data
lineitem_query = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE <= '1995-10-01'
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)
mysql_conn.close()

# Connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis to get part data
part_table = redis_conn.get('part')
part_df = pd.read_json(part_table, orient='split')

# Combine lineitem_df and part_df to calculate the promotion effect
combined_df = lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue and total revenue
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
total_revenue = combined_df['REVENUE'].sum()

# Calculate the promotional revenue
promotional_revenue = combined_df[combined_df['P_COMMENT'].str.contains(".*Promo.*")]['REVENUE'].sum()

# Calculate promotion effect percentage
promotion_effect = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Write results to a CSV file
with open("query_output.csv", "w") as f:
    f.write(f"Promotion Effect Percentage,{promotion_effect}\n")
