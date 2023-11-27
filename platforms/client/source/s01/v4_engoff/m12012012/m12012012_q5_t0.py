import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve ASIA nations from MongoDB
asia_nations = list(mongo_db.nation.find({'N_REGIONKEY': {'$eq': 2}}, {'N_NATIONKEY': 1, 'N_NAME': 1}))

# Create pandas dataframe for region from Redis
region_df = pd.read_json(redis_client.get('region'))

# Get customers and suppliers in ASIA
asia_nation_keys = [n['N_NATIONKEY'] for n in asia_nations]

# Query MySQL for customers in ASIA
mysql_cursor.execute(
    f"SELECT C_CUSTKEY FROM customer WHERE C_NATIONKEY IN {tuple(asia_nation_keys)}"
)
customers_in_asia = {row[0] for row in mysql_cursor.fetchall()}

# Query MongoDB for suppliers in ASIA
suppliers_in_asia = {d['S_SUPPKEY'] for d in mongo_db.supplier.find({'S_NATIONKEY': {'$in': asia_nation_keys}})}

# Retrieve lineitem from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter orders based on the date
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1990-01-01') &
    (lineitem_df['L_SHIPDATE'] <= '1995-01-01') &
    (lineitem_df['L_SUPPKEY'].isin(suppliers_in_asia)) &
    (lineitem_df['L_ORDERKEY'].isin(customers_in_asia))
]
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Aggregate revenue by nation
agg_revenue = filtered_lineitem_df.groupby(['L_SUPPKEY'], as_index=False)['REVENUE'].sum()

# Add nation names to the result
agg_revenue['N_NAME'] = agg_revenue['L_SUPPKEY'].apply(lambda skey: next((n['N_NAME'] for n in asia_nations if n['N_NATIONKEY'] == skey), None))

# Sort by revenue and select required columns
final_df = agg_revenue[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write result to csv
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
