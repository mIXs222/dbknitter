import pymysql
import pymongo
import pandas as pd
import direct_redis

# Function to get suppliers from MySQL
def get_mysql_suppliers(connection):
    with connection.cursor() as cursor:
        # Get suppliers from Canada
        cursor.execute("""
            SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS
            FROM supplier AS s JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
            WHERE n.N_NAME = 'CANADA'
        """)
        suppliers = cursor.fetchall()
    return suppliers

# Function to get part keys from Redis with part names starting with 'forest'
def get_redis_partkeys(redis_conn):
    partkeys = []
    for key in redis_conn.scan_iter(match='part:*'):
        part = redis_conn.hgetall(key)
        if part['P_NAME'].decode('utf-8').startswith('forest'):
            partkeys.append(int(part['P_PARTKEY']))
    return partkeys

# Function to get a threshold quantity from Redis
def get_redis_threshold(redis_conn, partkeys):
    thresholds = {}
    for key in redis_conn.scan_iter(match='lineitem:*'):
        lineitem = redis_conn.hgetall(key)
        partkey = int(lineitem['L_PARTKEY'])
        if partkey in partkeys and '1994-01-01' <= lineitem['L_SHIPDATE'].decode('utf-8') <= '1995-01-01':
            key = (partkey, int(lineitem['L_SUPPKEY']))
            thresholds[key] = thresholds.get(key, 0) + float(lineitem['L_QUANTITY'])
    # Calculate 50% threshold for each part-supplier combination
    for key in thresholds:
        thresholds[key] *= 0.5
    return thresholds

# Main execution
if __name__ == '__main__':
    # Connect to MySQL
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

    # Connect to MongoDB
    mongo_client = pymongo.MongoClient('mongodb', 27017)
    mongo_db = mongo_client['tpch']

    # Connect to Redis
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Get suppliers from MySQL
    suppliers = get_mysql_suppliers(mysql_conn)

    # Get part keys from Redis
    partkeys = get_redis_partkeys(redis_conn)

    # Get threshold quantity from Redis
    thresholds = get_redis_threshold(redis_conn, partkeys)

    # Filter suppliers based on the availability of parts
    final_suppliers = []
    for suppkey, s_name, s_address in suppliers:
        for partkey in partkeys:
            if (partkey, suppkey) in thresholds:
                final_suppliers.append((s_name, s_address))
                break

    # Convert results to DataFrame and drop duplicates
    df = pd.DataFrame(final_suppliers, columns=['S_NAME', 'S_ADDRESS']).drop_duplicates()

    # Sort results by supplier name
    df.sort_values(by='S_NAME', inplace=True)

    # Write to CSV
    df.to_csv('query_output.csv', index=False)

    # Close connections
    mysql_conn.close()
    mongo_client.close()
    redis_conn.close()
