import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch MySQL data
mysql_cursor.execute("SELECT C_CUSTKEY, C_NATIONKEY FROM customer;")
customer_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NATIONKEY'])

mysql_cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier;")
supplier_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# Fetch MongoDB data
nation_data = pd.DataFrame(list(mongodb_db.nation.find({}, {'_id': False})))
region_data = pd.DataFrame(list(mongodb_db.region.find({'R_NAME': 'ASIA'}, {'_id': False})))

# Close MongoDB connection
mongodb_client.close()

# Redis connection and fetch Redis data
redis_conn = DirectRedis(host='redis', port=6379, db=0)

orders_data = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_data = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Close Redis connection
redis_conn.close()

# Combine all dataframes
combined_df = (
    customer_data.merge(orders_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_data, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_data, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filtering
filtered_df = combined_df.query("O_ORDERDATE >= '1990-01-01' and O_ORDERDATE < '1995-01-01'")

# Calculating revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by N_NAME and sum REVENUE, then sort
result_df = (
    filtered_df.groupby('N_NAME', as_index=False)['REVENUE']
    .sum()
    .sort_values(by='REVENUE', ascending=False)
)

# Saving the result to CSV
result_df.to_csv('query_output.csv', index=False)
