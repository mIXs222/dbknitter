# query.py

import pymysql
import pandas as pd
import pymongo
from direct_redis import DirectRedis
import csv

# Connect to MySQL
my_sql_conn = pymysql.connect(database='tpch', user='root', password='my-secret-pw', host='mysql')
mysql_cursor = my_sql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for part table, filtered by P_TYPE
mysql_cursor.execute("SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE '%forest%'")
part_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME'])

# Query MongoDB for lineitem
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({
    'L_SHIPDATE': {
        '$gte': '1994-01-01',
        '$lt': '1995-01-01'
    }
})))

# Query MongoDB for partsupp
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Join part_df with lineitem_df on P_PARTKEY = L_PARTKEY
joined_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Aggregation to calculate total supplied quantity for each part and supplier
aggregation = joined_df.groupby(['L_SUPPKEY', 'P_PARTKEY']).agg({'L_QUANTITY': 'sum'}).reset_index()

# Query Redis for supplier data
suppliers_df = pd.DataFrame.from_records(
    [eval(redis_client.get('supplier:' + str(i))) for i in range(1, 100)], # Assume keys are 'supplier:1', 'supplier:2',...
    columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
)

# Query Redis for nation data, assuming we want nation CANADA
canada_nation = eval(redis_client.get('nation'))

# Filter suppliers in CANADA
suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'] == canada_nation['N_NATIONKEY']]

# Filter partsupp entries to include only the suppliers in Canada
partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(suppliers_df['S_SUPPKEY'])]

# Combine partsupp and aggregated lineitem data
combined_df = partsupp_df.merge(aggregation, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['P_PARTKEY', 'L_SUPPKEY'])

# Identify suppliers with excess parts
excess_suppliers = combined_df[combined_df['PS_AVAILQTY'] > (1.5 * combined_df['L_QUANTITY'])]

# Select the relevant columns and write to CSV
excess_suppliers[['PS_PARTKEY', 'PS_SUPPKEY']].to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Closing connections
mysql_cursor.close()
my_sql_conn.close()
mongo_client.close()
