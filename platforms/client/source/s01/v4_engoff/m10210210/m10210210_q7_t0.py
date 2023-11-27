import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and lineitem data
conn_mysql = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
query_mysql = """
    SELECT
        L_SUPPKEY,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS revenue,
        YEAR(L_SHIPDATE) AS year,
        L_SHIPDATE
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';
"""
lineitem_data = pd.read_sql(query_mysql, conn_mysql)
conn_mysql.close()

# MongoDB connection and nation, supplier data
client_mongo = pymongo.MongoClient('mongodb', 27017)
db_mongo = client_mongo['tpch']
nation_data = pd.DataFrame(list(db_mongo.nation.find({}, {'_id': 0})))
supplier_data = pd.DataFrame(list(db_mongo.supplier.find({}, {'_id': 0})))
client_mongo.close()

# Redis connection and customer data
redis_conn = DirectRedis(port=6379, host='redis')
customer_data = pd.read_json(redis_conn.get('customer'))

# Joining MongoDB and MySQL data
nation_supplier = pd.merge(
    supplier_data,
    nation_data.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPPLIER_NATION'}),
    on='S_NATIONKEY'
)

combined_data = pd.merge(
    lineitem_data,
    nation_supplier[['S_SUPPKEY', 'SUPPLIER_NATION']],
    on='L_SUPPKEY'
)

# Joining Redis data
combined_data = pd.merge(
    combined_data,
    customer_data.rename(columns={'C_NATIONKEY': 'CUSTOMER_NATIONKEY', 'C_NAME': 'CUSTOMER_NATION'}),
    left_on='L_ORDERKEY',
    right_on='C_CUSTKEY',
    how='inner'
)

# Filtering India and Japan trade
india_japan_trade = combined_data[
    ((combined_data['SUPPLIER_NATION'] == 'INDIA') & (combined_data['CUSTOMER_NATION'] == 'JAPAN')) |
    ((combined_data['SUPPLIER_NATION'] == 'JAPAN') & (combined_data['CUSTOMER_NATION'] == 'INDIA'))
]

# Grouping data by supplier nation, customer nation, and year
results = india_japan_trade.groupby(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'year']).agg({
    'revenue': 'sum'
}).reset_index()

# Sorting results
results.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'year'], inplace=True)

# Writing to CSV
results.to_csv('query_output.csv', index=False)
