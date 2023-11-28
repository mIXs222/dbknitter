import pymysql
import pymongo
import pandas as pd
import csv
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query to get lineitem and orders that match 'dim'
with mysql_conn:
    sql_query = """
    SELECT
        l.L_ORDERKEY,
        o.O_ORDERDATE,
        l.L_EXTENDEDPRICE,
        l.L_DISCOUNT,
        l.L_QUANTITY,
        o.O_CUSTKEY
    FROM
        lineitem l
    JOIN
        orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    WHERE
        l.L_PARTKEY IN (
            SELECT P_PARTKEY FROM part WHERE P_NAME like '%dim%'
        )
    """
    lineitem_orders = pd.read_sql(sql_query, mysql_conn)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Get 'part' data with the term 'dim'
part_data = list(mongodb['part'].find({'P_NAME': {'$regex': 'dim'}}))
part_df = pd.DataFrame(part_data)

# Form part keys list to filter lineitem_orders
part_keys_list = part_df['P_PARTKEY'].tolist()
lineitem_orders_dim = lineitem_orders[lineitem_orders['L_PARTKEY'].isin(part_keys_list)]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get 'supplier' and 'partsupp' data from Redis
supplier_data = pd.read_json(redis_client.get('supplier'))
partsupp_data = pd.read_json(redis_client.get('partsupp'))

# Merge Redis data with existing data
lineitem_orders_supplied = pd.merge(lineitem_orders_dim, partsupp_data, how='inner', left_on='L_PARTKEY', right_on='PS_PARTKEY')
lineitem_orders_supplied = pd.merge(lineitem_orders_supplied, supplier_data, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate profit for each line item
lineitem_orders_supplied['PROFIT'] = (lineitem_orders_supplied['L_EXTENDEDPRICE'] * (1 - lineitem_orders_supplied['L_DISCOUNT']) 
                                       - (lineitem_orders_supplied['PS_SUPPLYCOST'] * lineitem_orders_supplied['L_QUANTITY']))

# Get 'nation' data from mongodb
nation_df = pd.DataFrame(list(mongodb['nation'].find()))

# Merge nations to get the nation name
lineitem_orders_nation = pd.merge(lineitem_orders_supplied, nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Extract year from O_ORDERDATE
lineitem_orders_nation['YEAR'] = pd.to_datetime(lineitem_orders_nation['O_ORDERDATE']).dt.year

# Group by nation and year and sum profit
grouped_profit = lineitem_orders_nation.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort results by nation asc and year desc
sorted_profit = grouped_profit.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write to CSV
sorted_profit.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
