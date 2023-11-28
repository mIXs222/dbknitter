import pymysql
import pymongo
import pandas as pd
from bson import ObjectId
import direct_redis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_tpch = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for suppliers from Canada
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS
        FROM supplier AS s 
        WHERE s.S_NATIONKEY IN ( 
            SELECT N_NATIONKEY 
            FROM nation 
            WHERE N_NAME = 'CANADA' 
        )
    """)
    suppliers_in_canada = cursor.fetchall()

# Query Redis for the nation table
nation_df = pd.read_json(redis_client.get('nation'))

# Filter nation_df for Canada
canadian_nationkeys = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].tolist()

# Filter suppliers_in_canada for those in canadian_nationkeys
suppliers_in_canada_df = pd.DataFrame(suppliers_in_canada, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS'])
suppliers_in_canada_df = suppliers_in_canada_df[suppliers_in_canada_df['S_SUPPKEY'].isin(canadian_nationkeys)]

# Query MongoDB for line items
lineitem_conditions = {
    "L_SHIPDATE": {"$gte": ObjectId.from_datetime(pd.Timestamp('1994-01-01').to_pydatetime()),
                   "$lt": ObjectId.from_datetime(pd.Timestamp('1995-01-01').to_pydatetime())}
}
lineitem_quantities = mongo_tpch['lineitem'].aggregate([
    {'$match': lineitem_conditions},
    {'$group': {
        '_id': {'L_PARTKEY': '$L_PARTKEY', 'L_SUPPKEY': '$L_SUPPKEY'},
        'SUM_QUANTITY': {'$sum': '$L_QUANTITY'}
    }}
])

# Filter for threshold quantity
threshold_quantities = {item['_id']: item['SUM_QUANTITY'] for item in lineitem_quantities if item['SUM_QUANTITY'] >= 50}

# Query MySQL for partsupp with parts starting with 'forest'
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT PS_SUPPKEY, PS_PARTKEY, PS_AVAILQTY
        FROM partsupp
        WHERE PS_PARTKEY IN ( 
            SELECT P_PARTKEY 
            FROM part 
            WHERE P_NAME LIKE 'forest%'
        )
    """)
    partsupp_records = cursor.fetchall()

# Filter partsupp for threshold quantities
threshold_partsupp_df = pd.DataFrame(partsupp_records, columns=['PS_SUPPKEY', 'PS_PARTKEY', 'PS_AVAILQTY'])
threshold_partsupp_df = threshold_partsupp_df[
    threshold_partsupp_df.apply(lambda row: (row['PS_PARTKEY'], row['PS_SUPPKEY']) in threshold_quantities, axis=1)
]

# Final join to get supplier names and addresses
final_suppliers_df = suppliers_in_canada_df.merge(threshold_partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY', how='inner')

# Write the results to a CSV file
final_suppliers_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME').to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close connections
mysql_connection.close()
mongo_client.close()
redis_client.close()
