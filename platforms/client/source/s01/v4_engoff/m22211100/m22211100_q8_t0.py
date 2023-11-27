import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query for MySQL
mysql_query = """
SELECT 
    L_ORDERKEY, 
    O_ORDERDATE, 
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS REVENUE
FROM 
    orders, 
    lineitem
WHERE 
    O_ORDERKEY = L_ORDERKEY AND 
    O_ORDERDATE LIKE '%1995%' OR O_ORDERDATE LIKE '%1996%'
"""

# Execute MySQL Query
mysql_cursor.execute(mysql_query)

# Fetch results
mysql_results = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'O_ORDERDATE', 'REVENUE'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Query for MongoDB
supplier_results = pd.DataFrame(list(mongo_db.supplier.find({'S_NATIONKEY': 'INDIA'}, {'_id': 0})))

# Get data from Redis
nation_data = pd.read_json(redis_client.get('nation'), orient='records')
region_data = pd.read_json(redis_client.get('region'), orient='records')

# Merge data from MongoDB and Redis
nation_supplier_merge = pd.merge(supplier_results, nation_data, left_on='S_SUPPKEY', right_on='N_NATIONKEY')
asia_nations = region_data[region_data['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()
asia_suppliers = nation_supplier_merge[
    nation_supplier_merge['N_REGIONKEY'].isin(asia_nations)
]

# Combine the results
combined_results = pd.merge(mysql_results, asia_suppliers, left_on='L_ORDERKEY', right_on='S_SUPPKEY')

# Filter data for years and calculate market share
market_share_by_year = combined_results.groupby(combined_results['O_ORDERDATE'].str[:4])['REVENUE'].sum()

# Write results to CSV file
market_share_by_year.to_csv('query_output.csv', header=False)

# Print output for verification
print(market_share_by_year)
