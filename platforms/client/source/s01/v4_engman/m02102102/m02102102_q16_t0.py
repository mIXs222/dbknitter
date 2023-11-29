import pandas as pd
import pymysql
import pymongo
import direct_redis

# Function to connect to MySQL and retrieve suppliers with NO complaints (BBB)
def get_mysql_suppliers():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT S_SUPPKEY
                FROM supplier
                WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'
            """
            cursor.execute(sql)
            suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY'])
    finally:
        connection.close()
    return suppliers

# Function to connect to MongoDB and retrieve parts data
def get_mongodb_parts():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    query = {
        'P_SIZE': { '$in': [49, 14, 23, 45, 19, 3, 36, 9] },
        'P_TYPE': { '$ne': 'MEDIUM POLISHED' },
        'P_BRAND': { '$ne': 'Brand#45' }
    }
    parts = pd.DataFrame(list(db.part.find(query, {'P_PARTKEY': 1})))
    client.close()
    return parts

# Function to connect to Redis and retrieve the partsupp data
def get_redis_partsupp():
    rdr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    partsupp_data = pd.read_pickle(rdr.get('partsupp'))
    return partsupp_data

# Retrieve data from MySQL, MongoDB, and Redis
suppliers = get_mysql_suppliers()
parts = get_mongodb_parts()
partsupp = get_redis_partsupp()

# Merging data parts and suppliers based on partsupp (intersection of partkey and suppkey)
suppliers_partsupp = partsupp.merge(suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_data = suppliers_partsupp.merge(parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Count the number of suppliers per part
result = merged_data.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(Supplier_Count=('S_SUPPKEY', 'nunique'))
result_sorted = result.reset_index().sort_values(by=['Supplier_Count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output the result to query_output.csv
result_sorted.to_csv('query_output.csv', index=False)
