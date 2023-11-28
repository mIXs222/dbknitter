import pandas as pd
import pymysql
import pymongo
import direct_redis

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MySQL queries to fetch data from 'supplier' and 'nation' tables
mysql_cursor.execute("SELECT * FROM nation WHERE N_NAME = 'CANADA'")
nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

mysql_cursor.execute("SELECT * FROM supplier")
suppliers = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Joining 'nation' with 'supplier' based on 'N_NATIONKEY' and 'S_NATIONKEY'
canadian_suppliers = pd.merge(nations, suppliers, left_on='N_NATIONKEY', right_on='S_NATIONKEY')

# MongoDB queries to fetch data from 'partsupp' and 'lineitem'
partsupp_collection = mongodb_db['partsupp']
partsupp_data = list(partsupp_collection.find())
partsupp_df = pd.DataFrame(partsupp_data)

lineitem_collection = mongodb_db['lineitem']
lineitem_data = list(lineitem_collection.find({'L_SHIPDATE': {'$gte': '1994-01-01', '$lte': '1995-01-01'}}))
lineitem_df = pd.DataFrame(lineitem_data)

# Redis query to fetch 'part' data
parts_data = redis_client.get('part')
parts_df = pd.read_json(parts_data, orient='records')

# Filtering parts_df for part names starting with 'forest'
forest_parts_df = parts_df[parts_df['P_NAME'].str.startswith('forest')]

# Joining 'partsupp' with 'forest_parts_df' to obtain PS_SUPPKEY for forest parts
forest_parts_supp_df = pd.merge(partsupp_df, forest_parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Identifying the supplier keys from the 'forest_parts_supp_df'
suppliers_eligible_keys = forest_parts_supp_df['PS_SUPPKEY'].unique()

# Filtering suppliers that match the eligible keys and 'S_NATIONKEY' from 'canadian_suppliers'
canadian_eligible_suppliers = canadian_suppliers[canadian_suppliers['S_SUPPKEY'].isin(suppliers_eligible_keys)]

# Filtering lineitem_df for part-supplier combinations and calculating the threshold quantity
quantity_threshold_df = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
quantity_threshold_df['THRESHOLD_QUANTITY'] = quantity_threshold_df['L_QUANTITY'] * 0.5

# Identifying supplier keys which have quantities above the threshold
high_quantity_suppliers = quantity_threshold_df[quantity_threshold_df['L_QUANTITY'] > quantity_threshold_df['THRESHOLD_QUANTITY']]['L_SUPPKEY']

# Filtering suppliers that have more than the threshold quantity
final_suppliers = canadian_eligible_suppliers[canadian_eligible_suppliers['S_SUPPKEY'].isin(high_quantity_suppliers)]

# Selecting the relevant columns and sorting by 'S_NAME'
output_df = final_suppliers[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Writing the final result to 'query_output.csv'
output_df.to_csv('query_output.csv', index=False)

# Closing all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
