import pymysql
import pymongo
import pandas as pd
import direct_redis
import datetime
import csv

# Function to convert MongoDB's ISO date to a string year format 'YYYY'
def get_year_from_iso_date(iso_date):
    if iso_date:
        return iso_date.strftime('%Y')
    return None

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Execute MySQL query
mysql_query = '''
SELECT N_NAME AS NATION, S_NATIONKEY
FROM nation
WHERE N_NAME LIKE '%'
'''
mysql_cursor.execute(mysql_query)
nations = {row[1]: row[0] for row in mysql_cursor.fetchall()}

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
part_col = mongodb_db['part']
parts = {
    part['P_PARTKEY']: part
    for part in part_col.find({"P_NAME": {"$regex": ".*dim.*"}})
}

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_data = pd.read_json(redis_conn.get('partsupp'))
lineitem_data = pd.read_json(redis_conn.get('lineitem'))

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Preparing the dataframes
suppliers_data_query = '''
SELECT S_SUPPKEY, S_NATIONKEY
FROM supplier
'''

orders_data_query = '''
SELECT O_ORDERKEY, O_ORDERDATE
FROM orders
'''

# Fetching the required data from MySQL
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(suppliers_data_query)
        suppliers_data = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])
        cursor.execute(orders_data_query)
        orders_data = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_ORDERDATE'])

# Merge Redis and MySQL DataFrames
merged_data = (
    lineitem_data.merge(partsupp_data, how='inner', on=['PS_PARTKEY', 'PS_SUPPKEY'])
    .merge(orders_data, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(suppliers_data, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
)

# Filter parts and extract year from orderdate
merged_data = merged_data[merged_data['L_PARTKEY'].isin(parts.keys())]
merged_data['O_YEAR'] = merged_data['O_ORDERDATE'].apply(get_year_from_iso_date)

# Calculating Amount
merged_data['AMOUNT'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT']) - merged_data['PS_SUPPLYCOST'] * merged_data['L_QUANTITY']

# Grouping the data for final output
result_df = (
    merged_data.groupby(['S_NATIONKEY', 'O_YEAR'])
    ['AMOUNT']
    .sum()
    .reset_index()
)

# Replace Nation keys with Nation names
result_df['NATION'] = result_df['S_NATIONKEY'].map(nations)

# Selecting the final columns and ordering
result_df = result_df[['NATION', 'O_YEAR', 'AMOUNT']]
result_df = result_df.sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

# Output to CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
