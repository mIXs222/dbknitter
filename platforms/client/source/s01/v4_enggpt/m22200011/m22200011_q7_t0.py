# query.py
import pymysql
import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    query = """
    SELECT s.S_NATIONKEY, c.C_NATIONKEY, YEAR(l.L_SHIPDATE) as year, 
           SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM supplier s
    JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
    JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
    WHERE c.C_NATIONKEY = s.S_NATIONKEY
    AND s.S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME IN ('JAPAN', 'INDIA'))
    AND l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    GROUP BY s.S_NATIONKEY, c.C_NATIONKEY, year
    ORDER BY s.S_NATIONKEY, c.C_NATIONKEY, year;
    """
    cursor.execute(query)
    mysql_data = cursor.fetchall()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['supplier_nation', 'customer_nation', 'year', 'revenue'])

# Retrieve data from MongoDB
orders_col = mongo_db['orders']
lineitem_col = mongo_db['lineitem']

# Using aggregation to filter and join the required collections
pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'lineitems.L_SHIPDATE': {'$gte': '1995-01-01', '$lte': '1996-12-31'},
            'O_CUSTKEY': {'$exists': True}  # Assumes that customer nation is in the orders table
            # Append additional match conditions here if needed
        }
    },
    {
        '$project': {
            'year': {'$year': '$lineitems.L_SHIPDATE'},
            'revenue': {'$multiply': ['$lineitems.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitems.L_DISCOUNT']}]}
            # Project required fields here
        }
    }
]

mongo_data = list(orders_col.aggregate(pipeline))

# Convert MongoDB data to DataFrame
mongo_df = pd.DataFrame(mongo_data)

# Retrieve data from Redis (assuming Pandas DataFrame storage)
nation_df = pd.read_json(redis_client.get('nation'))

# Assuming `nation_df` has nation data, connect line items with nations based on suppkey and custkey.
# Process nation data and merge with MySQL and MongoDB DataFrames. 
# You need to adjust the code here based on actual data structure and requirements. 

# Combine all DataFrames
combined_df = pd.concat([mysql_df, mongo_df], ignore_index=True)

# Generate the detailed report as required
report_df = combined_df[(combined_df['supplier_nation'].isin(['JAPAN', 'INDIA'])) &
                        (combined_df['customer_nation'].isin(['JAPAN', 'INDIA']))]

# Exporting to CSV
report_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close all connections
mysql_conn.close()
mongo_client.close()
