import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = mysql_conn.cursor()
# Query for customer
cursor.execute(
    "SELECT C_CUSTKEY, C_NATIONKEY FROM customer"
)
customer_data = cursor.fetchall()

# Convert customer data to DataFrame
df_customer = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NATIONKEY'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
# Query for region and lineitem
region_data = list(mongodb.region.find({'R_NAME': 'ASIA'}))
df_region = pd.DataFrame(region_data, columns=['R_REGIONKEY', 'R_NAME'])

lineitem_data = list(mongodb.lineitem.find({
    'L_SHIPDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
}, {
    'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1
}))
df_lineitem = pd.DataFrame(lineitem_data)

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
df_nation = pd.read_json(redis_client.get('nation'))
df_supplier = pd.read_json(redis_client.get('supplier'))
df_orders = pd.read_json(redis_client.get('orders'))

# Merge and calculate the required output
result = (df_customer.merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
                    .merge(df_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
                    .merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
                    .merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
                    .merge(df_supplier, left_on='N_NATIONKEY', right_on='S_NATIONKEY'))

# Filtering suppliers within ASIA
result = result[result['R_NAME'] == 'ASIA']

# Calculate revenue
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
result = result.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort by revenue
result = result.sort_values(by='REVENUE', ascending=False)

# Output to a CSV file
result.to_csv('query_output.csv', index=False)

# Close connections
cursor.close()
mysql_conn.close()
mongo_client.close()
redis_client.close()
