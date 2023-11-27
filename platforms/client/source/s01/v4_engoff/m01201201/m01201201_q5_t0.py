import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

with mysql_conn.cursor() as cursor:
    # Query nations and regions from MySQL
    query_nation = """
    SELECT n.N_NATIONKEY, n.N_NAME
    FROM nation n
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE r.R_NAME = 'ASIA'
    """
    cursor.execute(query_nation)
    nations = cursor.fetchall()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get lineitems from MongoDB
lineitems = pd.DataFrame(list(mongo_db.lineitem.find({
    "L_SHIPDATE": {"$gte": "1990-01-01", "$lt": "1995-01-01"}
})))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Assuming customer data is stored as a JSON string under key 'customer'
customer_data = redis_conn.get('customer')
customers = pd.read_json(customer_data, typ='frame')

# Close the MySQL connection
mysql_conn.close()

# Merge the dataframes
results = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
results = results.merge(customers, how='inner', left_on='N_NATIONKEY', right_on='C_NATIONKEY')

# Filtering customers by nation and perform join with lineitems
results = results.merge(lineitems, how='inner', left_on='C_CUSTKEY', right_on='L_ORDERKEY')

# Calculate revenue volume
results['REVENUE'] = results['L_EXTENDEDPRICE'] * (1 - results['L_DISCOUNT'])
nation_revenue = results.groupby('N_NAME').agg({'REVENUE': 'sum'}).reset_index()

# Sort by revenue
nation_revenue_sorted = nation_revenue.sort_values(by='REVENUE', ascending=False)

# Write to CSV
nation_revenue_sorted.to_csv('query_output.csv', index=False)
