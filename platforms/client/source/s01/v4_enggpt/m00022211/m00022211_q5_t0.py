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

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL query
mysql_query = """
SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM nation n
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE r.R_NAME = 'ASIA'
GROUP BY n.N_NAME
ORDER BY revenue DESC
"""

# Perform query in mysql
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    nations = cursor.fetchall()

# Perform query in mongo for orders and lineitem
orders = pd.DataFrame(list(mongo_db.orders.find(
    {"O_ORDERDATE": {"$gte": "1990-01-01", "$lte": "1994-12-31"}},
    {"_id": 0, "O_ORDERKEY": 1}
)))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find(
    {},
    {"_id": 0, "L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}
)))

# Merge orders and lineitem collections on O_ORDERKEY and L_ORDERKEY
merged_mongo_data = pd.merge(
    orders,
    lineitem,
    how='inner',
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Calculate revenue from merged MongoDB data
mongo_revenue = merged_mongo_data.groupby(["L_ORDERKEY"])["L_EXTENDEDPRICE", "L_DISCOUNT"].apply(
    lambda x: (x["L_EXTENDEDPRICE"] * (1 - x["L_DISCOUNT"])).sum()
).reset_index(name='revenue')

# Get supplier and customer data from Redis and convert to Pandas DataFrame
supplier_data = pd.read_json(redis_conn.get('supplier'))
customer_data = pd.read_json(redis_conn.get('customer'))

# Close MySQL connection
mysql_conn.close()

# Transform MySQL data to DataFrame
nation_revenue_df = pd.DataFrame(nations, columns=['N_NAME', 'revenue'])

# Combine MongoDB data with Redis data
combined_data = nation_revenue_df.merge(mongo_revenue, left_on='N_NAME', right_on='L_ORDERKEY', how='left')

# Summarize the revenue data by nation
final_result = combined_data.groupby("N_NAME")["revenue"].sum().sort_values(ascending=False).reset_index()

# Write final result to the CSV file
final_result.to_csv('query_output.csv', index=False)
