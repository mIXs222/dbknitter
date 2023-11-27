import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to query the `part` table in MySQL
def query_mysql():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )
    try:
        part_sql = """
        SELECT * FROM part WHERE
        (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
        (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
        (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
        """
        with connection.cursor() as cursor:
            cursor.execute(part_sql)
            part_records = cursor.fetchall()
    finally:
        connection.close()
    return part_records

# Function to query the `lineitem` table in Redis
def query_redis(part_keys):
    redis_db = DirectRedis(host='redis', port=6379, db=0)
    lineitem_records = []
    for part_key in part_keys:
        lineitem_pd = pd.read_msgpack(redis_db.get('lineitem:' + str(part_key)))
        lineitem_records.append(lineitem_pd)
    return pd.concat(lineitem_records, ignore_index=True)

# Get part data from MySQL and lineitem data from Redis
parts = query_mysql()
part_keys = [part[0] for part in parts]
lineitems = query_redis(part_keys)

# Perform the operation to calculate discounted revenue on the combined data
result = lineitems[
    (lineitems['L_QUANTITY'].between(1, 11) | lineitems['L_QUANTITY'].between(10, 20) | lineitems['L_QUANTITY'].between(20, 30)) &
    ((lineitems['L_SHIPMODE'] == 'AIR') | (lineitems['L_SHIPMODE'] == 'AIR REG')) &
    lineitems['L_SHIPINSTRUCT'].str.contains('DELIVER IN PERSON')
]
result['DISCOUNTED_REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Output the result to CSV
result.to_csv('query_output.csv', index=False)
