# python_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Retrieve part information from MySQL
mysql_db_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

with pymysql.connect(**mysql_db_config) as mysql_conn:
    part_query = "SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE '%dim%'"
    parts_df = pd.read_sql(part_query, mysql_conn)

# Retrieve orders and lineitem information from MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_col = mongodb_db['orders']
lineitem_col = mongodb_db['lineitem']

pipeline = [
    {"$match": {"O_ORDERDATE": {"$exists": True}}},  # Assume orders have order date
    {"$lookup": {
        "from": "lineitem",
        "localField": "O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitems"
    }}
]

orders_lineitems_df = pd.DataFrame(list(orders_col.aggregate(pipeline)))

# Filter lineitems for parts with 'dim' in their name
lineitems_dim_df = orders_lineitems_df.explode('lineitems').reset_index()
lineitems_dim_df = lineitems_dim_df.merge(parts_df, left_on="lineitems.L_PARTKEY", right_on="P_PARTKEY")
lineitems_dim_df['year'] = pd.to_datetime(lineitems_dim_df['O_ORDERDATE']).dt.year

# Retrieve supplier and partsupp information from Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_conn.get('supplier'), orient='records')
partsupp_df = pd.read_json(redis_conn.get('partsupp'), orient='records')

# Compute profit per lineitem
profits_df = lineitems_dim_df.copy()
profits_df['supply_cost'] = profits_df.apply(lambda row: partsupp_df[
    (partsupp_df['PS_PARTKEY'] == row['lineitems']['L_PARTKEY']) &
    (supplier_df['S_SUPPKEY'] == row['lineitems']['L_SUPPKEY'])
]['PS_SUPPLYCOST'].iloc[0], axis=1)

profits_df['profit'] = profits_df.apply(lambda row: (row['lineitems']['L_EXTENDEDPRICE'] * (1 - row['lineitems']['L_DISCOUNT'])) -
                                  (row['supply_cost'] * row['lineitems']['L_QUANTITY']), axis=1)

# Group by nation and year
profits_df['nation'] = profits_df.apply(lambda row: supplier_df[supplier_df['S_SUPPKEY'] == row['lineitems']['L_SUPPKEY']]['S_NATIONKEY'].iloc[0], axis=1)
grouped_profit_df = profits_df.groupby(['nation', 'year']).sum()['profit'].reset_index()

# Sort results
sorted_profit_df = grouped_profit_df.sort_values(by=['nation', 'year'], ascending=[True, False])

# Output to CSV
sorted_profit_df.to_csv('query_output.csv', index=False)
