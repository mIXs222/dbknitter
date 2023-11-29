import pymysql
import pymongo
import pandas as pd
import csv
from datetime import datetime

# MySQL database credentials
mysql_creds = {
    'db': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# MongoDB connection information
mongodb_creds = {
    'db': 'tpch',
    'hostname': 'mongodb',
    'port': 27017,
}

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=mysql_creds['host'],
    user=mysql_creds['user'],
    password=mysql_creds['password'],
    db=mysql_creds['db'],
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=mongodb_creds['hostname'], port=mongodb_creds['port'])
mongo_db = mongo_client[mongodb_creds['db']]

# Execute SQL query on MySQL database
nation_query = """
SELECT
    n1.N_NATIONKEY
FROM
    nation n1
WHERE
    n1.N_NAME = 'INDIA';
"""
mysql_cursor.execute(nation_query)
india_nation_key = mysql_cursor.fetchone()[0]

# Build the MongoDB pipeline for aggregation
pipeline = [
    {'$match': 
        {'S_NATIONKEY' : india_nation_key}
    },
    {'$lookup': {
        'from': 'lineitem',
        'localField': 'S_SUPPKEY',
        'foreignField': 'L_SUPPKEY',
        'as': 'lineitem_docs'
    }},
    {'$unwind': '$lineitem_docs'},
    {'$lookup': {
        'from': 'orders',
        'localField': 'lineitem_docs.L_ORDERKEY',
        'foreignField': 'O_ORDERKEY',
        'as': 'order_docs'
    }},
    {'$unwind': '$order_docs'},
    {'$match': {
        'order_docs.O_ORDERDATE': {'$regex': '^(199[56])'}
    }},
    {
        '$project': {
            'year': {'$substr': ['$order_docs.O_ORDERDATE', 0, 4]},
            'revenue': {
                '$multiply': [
                    '$lineitem_docs.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitem_docs.L_DISCOUNT']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': '$year',
            'total_revenue': {'$sum': '$revenue'}
        }
    }
]

# Run the MongoDB pipeline
supplier_revenue_by_year = list(mongo_db['supplier'].aggregate(pipeline))

# Convert to DataFrame for processing
df = pd.DataFrame(supplier_revenue_by_year)
df.columns = ['year', 'market_share']
df = df[df['year'].isin(['1995', '1996'])]  # Filter for years 1995 and 1996

# Write the output to a CSV file
df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()
