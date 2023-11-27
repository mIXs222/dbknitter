import pandas as pd
import pymysql
import pymongo
import direct_redis

# Constants
MYSQL_CONN = {
    "database": "tpch",
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql"
}

MONGODB_CONN = {
    "database": "tpch",
    "port": 27017,
    "host": "mongodb"
}

REDIS_CONN = {
    "db": 0,
    "port": 6379,
    "host": "redis"
}

# Helper functions
def query_mysql(connection_info):
    connection = pymysql.connect(host=connection_info['host'],
                                 user=connection_info['user'],
                                 password=connection_info['password'],
                                 db=connection_info['database'])
    cursor = connection.cursor()

    # Assuming 'CANADA' corresponds to a specific NATIONKEY in the Nation table
    # 'CANADA_NATIONKEY' to be replaced with the correct NATIONKEY after fetching it from the Redis database
    cursor.execute("""
        SELECT 
            l.PS_SUPPKEY,
            SUM(l.L_QUANTITY) as total_quantity
        FROM 
            partsupp AS ps
        INNER JOIN 
            lineitem AS l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
        WHERE 
            l.L_SHIPDATE >= '1994-01-01' AND l.L_SHIPDATE < '1995-01-01' AND ps.PS_AVAILQTY > (0.5 * l.L_QUANTITY)
        GROUP BY 
            l.PS_SUPPKEY
    """)
    
    supplier_quantity = pd.DataFrame(cursor.fetchall(), columns=["S_SUPPKEY", "TOTAL_QUANTITY"])
    
    cursor.close()
    connection.close()

    return supplier_quantity

def query_mongodb(connection_info):
    client = pymongo.MongoClient(connection_info['host'], connection_info['port'])
    db = client[connection_info['database']]
    
    parts = pd.DataFrame(list(db.part.find({'P_NAME': {'$regex': '^.*forest.*$'}})))
    
    client.close()
    
    return parts

def query_redis(connection_info):
    client = direct_redis.DirectRedis(host=connection_info['host'], port=connection_info['port'], db=connection_info['db'])
    
    nations_df = pd.read_json(client.get("nation"))
    suppliers_df = pd.read_json(client.get("supplier"))
    
    client.close()
    
    return nations_df, suppliers_df

# Execute queries on various databases
supplier_quantity_mysql = query_mysql(MYSQL_CONN)
parts_mongodb = query_mongodb(MONGODB_CONN)
nation_redis, supplier_redis = query_redis(REDIS_CONN)

# Assuming 'CANADA' corresponds to a specific NATIONKEY in the nation table
canada_nationkey = nation_redis[nation_redis['N_NAME'] == 'CANADA']['N_NATIONKEY'].iloc[0]

# Filtering suppliers based on the Canada nation key
supplier_redis = supplier_redis[supplier_redis['S_NATIONKEY'] == canada_nationkey]

# Merging data
result_df = supplier_quantity_mysql.merge(supplier_redis, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
result_df = result_df.merge(parts_mongodb, left_on='S_SUPPKEY', right_on='P_PARTKEY')

# Filter out suppliers with excess forest parts
result_df = result_df[result_df['TOTAL_QUANTITY'] > (0.5 * result_df['P_SIZE'])]

# Save result to CSV
result_df.to_csv('query_output.csv', index=False)
