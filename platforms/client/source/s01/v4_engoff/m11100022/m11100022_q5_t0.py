import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve MySQL Data
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier")
    suppliers = cursor.fetchall()
    cursor.execute("SELECT C_CUSTKEY, C_NATIONKEY FROM customer")
    customers = cursor.fetchall()

supplier_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY'])
customer_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NATIONKEY'])

# Retrieve MongoDB Data
nations = list(mongo_db.nation.find({'N_REGIONKEY': {"$in": mongo_db.region.find({'R_NAME': 'ASIA'}, {'R_REGIONKEY': 1})}}, {'_id': 0}))
nation_df = pd.DataFrame(nations)

# Retrieve Redis Data
orders = pd.read_json(redis_client.get('orders'))
lineitem = pd.read_json(redis_client.get('lineitem'))
lineitem = lineitem[(lineitem['L_SHIPDATE'] >= '1990-01-01') & (lineitem['L_SHIPDATE'] <= '1995-01-01')]

# Close all connections
mysql_connection.close()
mongo_client.close()
redis_client.close()

# Define a formula to calculate revenue
def calculate_revenue(row):
    return row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])

# Filter the suppliers and customers from nations in ASIA
asia_nations = nation_df['N_NATIONKEY'].unique()
asia_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations)]
asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(asia_nations)]

# Join the dataframes to calculate the local supplier volume
lineitem['REVENUE'] = lineitem.apply(calculate_revenue, axis=1)
result = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result = result.merge(asia_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
result = result.merge(asia_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result = result.groupby('S_NATIONKEY')['REVENUE'].sum().reset_index()

# Get the nation names
result = result.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select relevant columns, sort, and save to CSV
result = result[['N_NAME', 'REVENUE']]
result = result.sort_values('REVENUE', ascending=False)
result.to_csv('query_output.csv', index=False)
