import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from functools import reduce

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Function for quering MySQL
def query_mysql(query):
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Function for querying MongoDB and converting to DataFrame
def query_mongo(collection, query):
     return pd.DataFrame(list(mongo_db[collection].find(query)))

# Querying MySQL tables
orders_query = "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE FROM orders WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'"
orders = pd.DataFrame(query_mysql(orders_query), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE'])

lineitem_query = "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT FROM lineitem"
lineitem = pd.DataFrame(query_mysql(lineitem_query), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Querying MongoDB tables
parts = query_mongo("part", {"P_TYPE": "SMALL PLATED COPPER"})
nations = query_mongo("nation", {})

# Querying Redis tables
supplier = pd.read_json(redis_client.get('supplier'), orient='index')
customer = pd.read_json(redis_client.get('customer'), orient='index')

# Merging DataFrames from multiple databases
def merge_dfs(*dfs, on, how='inner'):
    return reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)

# Merging orders and customers on O_CUSTKEY -> C_CUSTKEY
orders_customers = merge_dfs(orders, customer, on='O_CUSTKEY')
# Merging part and lineitem on P_PARTKEY -> L_PARTKEY
parts_lineitem = merge_dfs(parts, lineitem, on='P_PARTKEY')
# Merging lineitem and supplier on L_SUPPKEY -> S_SUPPKEY
lineitem_supplier = merge_dfs(parts_lineitem, supplier, on='L_SUPPKEY')

# Merge all based on keys
all_nations = merge_dfs(orders_customers, lineitem_supplier, on='O_ORDERKEY')

# Create calculated fields
all_nations['VOLUME'] = all_nations['L_EXTENDEDPRICE'] * (1 - all_nations['L_DISCOUNT'])
all_nations['O_YEAR'] = all_nations['O_ORDERDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)

# Filter by ASIA region
asia_customers = all_nations[all_nations['R_NAME'] == 'ASIA']
# Calculate market share
mkt_share = (asia_customers.groupby('O_YEAR')
             .apply(lambda x: pd.Series({
                 'MKT_SHARE': x[x['N_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()
             }))
             .reset_index())

# Output to CSV
mkt_share.to_csv('query_output.csv', index=False)
