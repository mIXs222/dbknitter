import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Fetch relevant data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
    part_keys = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
    nation_key = cursor.fetchone()[0]

# Build a part key string for IN clause
part_keys_str = ','.join(map(str, part_keys))

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Aggregate lineitem data from MongoDB
pipeline = [
    {"$match": {
        "L_PARTKEY": {"$in": part_keys},
        "L_SHIPDATE": {"$gte": '1994-01-01', "$lt": '1995-01-01'}
    }},
    {"$group": {
        "_id": {"L_PARTKEY": "$L_PARTKEY", "L_SUPPKEY": "$L_SUPPKEY"},
        "SUM_L_QUANTITY": {"$sum": "$L_QUANTITY"}
    }}
]
lineitems = list(mongodb_db['lineitem'].aggregate(pipeline))

# Process lineitem data 
lineitems_processed = {(item['_id']['L_PARTKEY'], item['_id']['L_SUPPKEY']): item['SUM_L_QUANTITY'] / 2 for item in lineitems}

# Connect to Redis using DirectRedis
r = direct_redis.DirectRedis(port=6379, host='redis')

# Get partsupp data from Redis
partsupp_df = pd.DataFrame(eval(r.get('partsupp')))
suppliers_df = pd.DataFrame(eval(r.get('supplier')))

# Filter partsupp data
condition = partsupp_df.apply(lambda row: (row['PS_PARTKEY'], row['PS_SUPPKEY']) in lineitems_processed and row['PS_AVAILQTY'] > lineitems_processed[(row['PS_PARTKEY'], row['PS_SUPPKEY'])], axis=1)
partsupp_df = partsupp_df[condition]

# Filter suppliers dataframe based on nation_key and S_SUPPKEY from partsupp_df
suppliers_df = suppliers_df[(suppliers_df['S_NATIONKEY'] == nation_key) & suppliers_df['S_SUPPKEY'].isin(partsupp_df['PS_SUPPKEY'])]

# Sort and select relevant columns for output
suppliers_df = suppliers_df.sort_values('S_NAME')[['S_NAME', 'S_ADDRESS']]

# Write results to CSV
suppliers_df.to_csv('query_output.csv', index=False)
