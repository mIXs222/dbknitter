import pandas as pd
import pymysql
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    database='tpch',
    user='root',
    password='my-secret-pw'
)

# Prepare the query to fetch lineitem data
mysql_query = """
SELECT L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""

# Run the query and fetch the data
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch the part data from Redis and convert it into a Pandas DataFrame
part_keys = redis_conn.smembers('part')
part_data = [eval(redis_conn.get(key)) for key in part_keys]
part_df = pd.DataFrame(part_data)

# Assuming promotional parts are identified with a specific flag in the P_COMMENT column
# (since the rule for identifying promotional parts is not provided)
promotional_part_keys = part_df[part_df['P_COMMENT'].str.contains('Promotion')]['P_PARTKEY']

# Filter the lineitem DataFrame for promotional parts
promotional_lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(promotional_part_keys)]

# Calculate the revenue
promotional_lineitem_df['REVENUE'] = promotional_lineitem_df['L_EXTENDEDPRICE'] * (1 - promotional_lineitem_df['L_DISCOUNT'])

# Calculate total revenue and promotional revenue
total_revenue = lineitem_df['L_EXTENDEDPRICE'].sum()
promotional_revenue = promotional_lineitem_df['REVENUE'].sum()

# Calculate the percentage of promotional revenue
percentage_promotional_revenue = (promotional_revenue / total_revenue) * 100

# Output to the file
output = pd.DataFrame([{'Percentage of Promotional Revenue': percentage_promotional_revenue}])
output.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
redis_conn.close()
