import pandas as pd
import pymysql
import pymongo
import direct_redis

# Establish connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to select from orders and lineitem in MySQL
mysql_query = """
SELECT
    o.O_ORDERKEY,
    o.O_CUSTKEY,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT
FROM
    orders o
JOIN
    lineitem l ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE
    o.O_ORDERDATE >= '1993-10-01'
    AND o.O_ORDERDATE <= '1993-12-31'
    AND l.L_RETURNFLAG = 'R'
"""
mysql_orders_lineitem = pd.read_sql(mysql_query, mysql_connection)
mysql_connection.close()

# Calculate revenue and drop unnecessary columns
mysql_orders_lineitem['REVENUE'] = mysql_orders_lineitem['L_EXTENDEDPRICE'] * (1 - mysql_orders_lineitem['L_DISCOUNT'])
mysql_orders_lineitem = mysql_orders_lineitem[['O_CUSTKEY', 'REVENUE']]

# Establish connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Query to select from nation in MongoDB
mongodb_nations = pd.DataFrame(list(mongodb_db.nation.find()))

# Establish connection to Redis
redis_db = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read customer data from Redis
customer_json_data = redis_db.get('customer')
customers_df = pd.read_json(customer_json_data)

# Merge MySQL and Redis data on customer key
merged_data = pd.merge(mysql_orders_lineitem, customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Merge in nation name from MongoDB data
merged_data = pd.merge(merged_data, mongodb_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Group by customer attributes
grouped_data = merged_data.groupby([
    'C_CUSTKEY',
    'C_NAME',
    'C_ACCTBAL',
    'C_PHONE',
    'N_NAME',
    'C_ADDRESS',
    'C_COMMENT'
]).agg({'REVENUE': 'sum'}).reset_index()

# Sort the data according to the specification
sorted_data = grouped_data.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write the results to CSV
sorted_data.to_csv('query_output.csv', index=False)
