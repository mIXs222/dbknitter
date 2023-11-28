# import libraries
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Establish connections to the databases
# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query data from MySQL
with mysql_conn.cursor() as cursor:
    # Query partsupp table
    cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp")
    partsupp = pd.DataFrame(cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY'])
    
    # Query lineitem table
    cursor.execute("""
    SELECT L_PARTKEY, L_SUPPKEY, SUM(L_QUANTITY) AS total_quantity
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    GROUP BY L_PARTKEY, L_SUPPKEY
    """)
    lineitem = pd.DataFrame(cursor.fetchall(), columns=['L_PARTKEY', 'L_SUPPKEY', 'total_quantity'])

# Filter lineitem for quantity threshold
lineitem = lineitem[lineitem['total_quantity'] > (lineitem['total_quantity'].sum() * 0.5)]

# Get parts from MongoDB that match the condition
parts = pd.DataFrame(list(mongo_db.part.find({"P_NAME": {"$regex": '^forest', "$options": "i"}})))

# Filter partsupp rows for parts obtained from mongodb
partsupp_filtered = partsupp[partsupp['PS_PARTKEY'].isin(parts['P_PARTKEY'])]

# Query Redis for nation and supplier data
nation_df = pd.read_json(redis_conn.get('nation'), orient='records')
supplier_df = pd.read_json(redis_conn.get('supplier'), orient='records')

# Filter for CANADA suppliers
canada_nations = nation_df[nation_df['N_NAME'] == 'CANADA']
canada_suppliers = supplier_df[(supplier_df['S_SUPPKEY'].isin(partsupp_filtered['PS_SUPPKEY'])) &
                               (supplier_df['S_NATIONKEY'].isin(canada_nations['N_NATIONKEY']))]

# Selecting supplier names and addresses
suppliers_filtered = canada_suppliers[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Output result to CSV
suppliers_filtered.to_csv('query_output.csv', index=False)

# Close connections
mongo_client.close()
mysql_conn.close()
