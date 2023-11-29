# query.py

import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', cursorclass=pymysql.cursors.Cursor)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Function to get data from MySQL
def get_mysql_data(specified_line):
    with mysql_conn.cursor() as cursor:
        # Construct the SQL query
        sql_query = f"""
        SELECT s.S_NATIONKEY, YEAR(o.O_ORDERDATE) as year, SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
        FROM partsupp ps
        JOIN lineitem l ON ps.PS_SUPPKEY = l.L_SUPPKEY AND ps.PS_PARTKEY = l.L_PARTKEY
        JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
        WHERE l.L_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_NAME like %s)
        GROUP BY s.S_NATIONKEY, year
        ORDER BY s.S_NATIONKEY ASC, year DESC
        """
        cursor.execute(sql_query, ('%' + specified_line + '%',))
        result = cursor.fetchall()

    # Convert to DataFrame
    mysql_df = pd.DataFrame(result, columns=['N_NATIONKEY', 'year', 'profit'])
    return mysql_df

# Function to get data from MongoDB
def get_mongo_data(specified_line):
    # Construct the Mongo query
    pipeline = [
        {
            '$match': {
                'L_PARTKEY': {
                    '$in': [p['P_PARTKEY'] for p in redis_client.get('part') if specified_line in p['P_NAME']]
                }
            }
        },
        {
            '$lookup': {
                'from': 'orders',
                'localField': 'L_ORDERKEY',
                'foreignField': 'O_ORDERKEY',
                'as': 'order_info'
            }
        },
        {'$unwind': '$order_info'},
        {
            '$group': {
                '_id': {
                    'N_NATIONKEY': '$L_SUPPKEY',  # Assuming L_SUPPKEY refers to the supplier's nation key
                    'year': {'$year': '$order_info.O_ORDERDATE'}
                },
                'profit': {
                    '$sum': {
                        '$subtract': [
                            {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]},
                            {'$multiply': ['$PS_SUPPLYCOST', '$L_QUANTITY']}  # Assuming PS_SUPPLYCOST exists on lineitem
                        ]
                    }
                }
            }
        },
        {'$sort': {'_id.N_NATIONKEY': 1, '_id.year': -1}}
    ]
    result = list(mongodb['lineitem'].aggregate(pipeline))

    # Convert to DataFrame
    mongo_df = pd.DataFrame(result)
    if not mongo_df.empty:
        mongo_df['N_NATIONKEY'] = mongo_df['_id'].apply(lambda x: x['N_NATIONKEY'])
        mongo_df['year'] = mongo_df['_id'].apply(lambda x: x['year'])
        mongo_df.drop('_id', axis=1, inplace=True)
    return mongo_df

# Main function to execute and combine queries
def main():
    specified_line = input("Please specify the product line: ")
    mysql_result = get_mysql_data(specified_line)
    mongo_result = get_mongo_data(specified_line)
    
    # Combine results
    combined_df = pd.concat([mysql_result, mongo_result])
    combined_df = combined_df.groupby(['N_NATIONKEY', 'year']).agg({'profit': 'sum'}).reset_index()
    combined_df.sort_values(by=['N_NATIONKEY', 'year'], ascending=[True, False], inplace=True)
    
    # Save to CSV
    combined_df.to_csv('query_output.csv', index=False)

main()
