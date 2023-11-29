import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to the MySQL server
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to get nations in ASIA region and all suppliers
mysql_query = '''
SELECT n.N_NAME, s.S_SUPPKEY
FROM nation n
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
WHERE r.R_NAME = 'ASIA';
'''

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    supplier_nation_data = cursor.fetchall() # Fetch the results

# Convert to DataFrame
supplier_nation_df = pd.DataFrame(supplier_nation_data, columns=['N_NAME', 'S_SUPPKEY'])

# Close the MySQL connection
mysql_connection.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")

# Selecting the tpch database and customer collection
mongo_db = mongo_client['tpch']
mongo_customer_collection = mongo_db['customer']

# Query to get customers in ASIA region
customer_data = mongo_customer_collection.find({
    'C_NATIONKEY': {'$in': supplier_nation_df['N_NATIONKEY'].tolist()}
}, {'_id': 0, 'C_CUSTKEY': 1})

# Convert to DataFrame
customer_df = pd.DataFrame(list(customer_data))

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_df = pd.DataFrame(eval(redis_connection.get('lineitem')))

# Filter lineitem data by date range and join with supplier and customer data
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= datetime(1990, 1, 1)) &
    (lineitem_df['L_SHIPDATE'] < datetime(1995, 1, 1)) &
    (lineitem_df['L_SUPPKEY'].isin(supplier_nation_df['S_SUPPKEY'])) &
    (lineitem_df['L_ORDERKEY'].isin(customer_df['C_CUSTKEY']))
]

# Calculate revenue
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Sum revenue by nation, sort by revenue in descending order
revenue_by_nation = filtered_lineitem_df.groupby('N_NAME')['REVENUE'].sum().reset_index()
revenue_by_nation_sorted = revenue_by_nation.sort_values('REVENUE', ascending=False)

# Save to CSV file
revenue_by_nation_sorted.to_csv('query_output.csv', index=False)
