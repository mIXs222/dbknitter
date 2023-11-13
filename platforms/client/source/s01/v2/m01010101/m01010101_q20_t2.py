import os
import pandas as pd
import pymongo
from dotenv import load_dotenv
import pymysql.cursors

# Load environment variables
load_dotenv()

# MySQL connection configuration
conn = pymysql.connect(host=os.getenv('MYSQL_HOST'),
                             user=os.getenv('MYSQL_USER'),
                             password=os.getenv('MYSQL_PASSWORD'),
                             db=os.getenv('MYSQL_DB'))

# Collect data from mysql tables: nation, part, partsupp
nation_df = pd.read_sql('SELECT * FROM nation', conn)
part_df = pd.read_sql('SELECT * FROM part where P_NAME LIKE "forest%"', conn)
partsupp_df = pd.read_sql('SELECT * FROM partsupp', conn)

# Close MySQL connection
conn.close()

# MongoDB connection configuration
client = pymongo.MongoClient(os.getenv('MONGODB_URL'))

# Mongo database and collection
supplier = client[os.getenv('MONGODB_DB')]['supplier']
lineitem = client[os.getenv('MONGODB_DB')]['lineitem']

# Collect data from mongodb tables: supplier, lineitem
supplier_df = pd.DataFrame(list(supplier.find({})))
lineitem_df = pd.DataFrame(list(lineitem.find({})))

# Close MongoDB connection
client.close()

# Join and filter data
supplier_nation_join = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
supplier_nation_partsupp_join = pd.merge(supplier_nation_join, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
supplier_nation_partsupp_part_join = pd.merge(supplier_nation_partsupp_join, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Filter based on the condition
final_df = supplier_nation_partsupp_part_join[supplier_nation_partsupp_part_join['PS_AVAILQTY'] > 0.5*lineitem_df['L_QUANTITY'].sum()]

# Write results to csv file
final_df[['S_NAME', 'S_ADDRESS']].to_csv('query_output.csv', index=False)
