import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Establish MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Establish Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Define the query for MySQL
mysql_query = """
SELECT p.P_TYPE, s.S_NATIONKEY, o.O_ORDERDATE, 
       l.L_EXTENDEDPRICE, l.L_DISCOUNT
FROM part p 
JOIN lineitem l ON p.P_PARTKEY = l.L_PARTKEY
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
"""

# Execute the MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitem_supplier_order_part_data = cursor.fetchall()

# Convert the result to a DataFrame
df_mysql = pd.DataFrame(lineitem_supplier_order_part_data, columns=['P_TYPE', 'S_NATIONKEY', 'O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Get documents from nation collection in MongoDB
nation_data = mongo_db.nation.find()
region_data = redis_client.get('region')

# Convert the MongoDB data to DataFrame
df_mongo = pd.DataFrame(list(nation_data))

# Convert the Redis data to DataFrame
df_redis = pd.read_msgpack(region_data)

# Merge the data from different sources
df_merged = df_mysql.merge(df_mongo, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_merged = df_merged.merge(df_redis, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter for SMALL PLATED COPPER, ASIA, INDIA, and the years 1995 and 1996
df_filtered = df_merged[(df_merged['P_TYPE'] == 'SMALL PLATED COPPER') & 
                        (df_merged['R_NAME'] == 'ASIA') & 
                        (df_merged['N_NAME'] == 'INDIA') & 
                        (df_merged['O_ORDERDATE'].dt.year.isin([1995, 1996]))]

# Calculate the revenue per year
df_filtered['REVENUE'] = df_filtered['L_EXTENDEDPRICE'] * (1 - df_filtered['L_DISCOUNT'])
df_revenue_per_year = df_filtered.groupby(df_filtered['O_ORDERDATE'].dt.year)['REVENUE'].sum().reset_index()

# Write output to CSV
df_revenue_per_year.columns = ['YEAR', 'MARKET_SHARE']
df_revenue_per_year.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
