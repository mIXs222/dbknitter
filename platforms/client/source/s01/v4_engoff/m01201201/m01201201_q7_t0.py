import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL query to select nation and supplier
mysql_query = """
SELECT n1.N_NAME AS SUPPLIER_NATION, n2.N_NAME AS CUSTOMER_NATION, YEAR(o.O_ORDERDATE) AS YEAR, 
l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS VOLUME
FROM supplier s
JOIN nation n1 ON s.S_NATIONKEY = n1.N_NATIONKEY
JOIN orders o ON s.S_SUPPKEY = l.L_SUPPKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN nation n2 ON c.C_NATIONKEY = n2.N_NATIONKEY
WHERE n1.N_NAME IN ('INDIA', 'JAPAN') 
AND n2.N_NAME IN ('INDIA', 'JAPAN') 
AND n1.N_NAME != n2.N_NAME
AND o.O_ORDERDATE >= '1995-01-01' AND o.O_ORDERDATE <= '1996-12-31';
"""

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             database='tpch')

# Create a DataFrame from MySQL data
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB and fetch lineitem data
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']
lineitem_data = lineitem_collection.find({'L_SHIPDATE': {'$gte': '1995-01-01', '$lte': '1996-12-31'}})
lineitem_df = pd.DataFrame(list(lineitem_data))

# Connect to Redis and read customer data into DataFrame
redis_conn = DirectRedis(host='redis', port=6379, db=0)
customer_json = redis_conn.get('customer')
customer_df = pd.read_json(customer_json)

# Merge the DataFrames
# Note: This sample assumes the lineitem dataframe has already been merged into mysql_df
# ... extra code here to perform any necessary merges using pandas ...

# Store final resulting DataFrame to a csv file
final_df.to_csv('query_output.csv', index=False)
