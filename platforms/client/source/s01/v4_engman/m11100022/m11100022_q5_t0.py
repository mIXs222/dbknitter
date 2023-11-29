import pymysql
import pymongo
import pandas as pd

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve nations and regions in ASIA from MongoDB
nations_df = pd.DataFrame(list(mongo_db.nation.find()))
regions_df = pd.DataFrame(list(mongo_db.region.find({"R_NAME": "ASIA"})))
asian_nations = nations_df[nations_df['N_REGIONKEY'].isin(regions_df['R_REGIONKEY'])]

# Retrieve customers in ASIA
query = """
SELECT C_CUSTKEY, C_NATIONKEY
FROM customer
WHERE C_NATIONKEY IN (%s)
"""
asian_customers_df = pd.read_sql(query % ','.join(map(str, asian_nations['N_NATIONKEY'])), mysql_conn)

# Retrieve suppliers in ASIA
query = """
SELECT S_SUPPKEY, S_NATIONKEY
FROM supplier
WHERE S_NATIONKEY IN (%s)
"""
asian_suppliers_df = pd.read_sql(query % ','.join(map(str, asian_nations['N_NATIONKEY'])), mysql_conn)

mysql_conn.close()

# Redis connection setup and data retrieval
import direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders in the specified date range
orders_df = pd.read_json(redis_conn.get('orders'))
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1990-01-01') & (orders_df['O_ORDERDATE'] <= '1995-01-01')]

# Get lineitem data
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter for asian customers
orders_df = orders_df[orders_df['O_CUSTKEY'].isin(asian_customers_df['C_CUSTKEY'])]

# Compute the revenue volume for lineitems with asian suppliers
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
asian_lineitems = lineitem_df[lineitem_df['L_SUPPKEY'].isin(asian_suppliers_df['S_SUPPKEY'])]
asian_lineitems = asian_lineitems[asian_lineitems['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'])]

# Calculate revenue by nation
revenue_by_nation = asian_lineitems.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()
revenue_by_nation = revenue_by_nation.merge(
    asian_suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY'
)[['S_NATIONKEY', 'REVENUE']]

# Summarize revenue by nation and sort
output_df = revenue_by_nation.groupby('S_NATIONKEY')['REVENUE'].sum().reset_index()
output_df = output_df.merge(asian_nations[['N_NATIONKEY', 'N_NAME']], left_on='S_NATIONKEY', right_on='N_NATIONKEY')
output_df = output_df[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
