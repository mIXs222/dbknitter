import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

def get_data_from_redis(table_name):
    df_json = redis_conn.get(table_name)
    return pd.read_json(df_json)

def execute_mysql_query(query):
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# region and nation join to filter by ASIA region
mysql_query_asia = """
SELECT N_NATIONKEY 
FROM nation N, region R
WHERE R.R_NAME = 'ASIA' AND N.N_REGIONKEY = R.R_REGIONKEY
"""
asian_nations = execute_mysql_query(mysql_query_asia)
asian_nation_keys = [row[0] for row in asian_nations]

# filter asian supplier from mongodb
suppliers_cursor = mongo_db['supplier'].find(
    {'S_NATIONKEY': {'$in': asian_nation_keys}, 'S_NAME': {'$regex': '.*INDA.*'}},
    {'S_SUPPKEY': 1}
)
indian_suppliers = [supplier['S_SUPPKEY'] for supplier in suppliers_cursor]

# filter parts from redis
parts_df = get_data_from_redis('part')
small_plated_copper_df = parts_df[
    (parts_df['P_TYPE'] == 'SMALL PLATED COPPER')
]

# Query lineitem table in MySQL for specified conditions
mysql_query_revenue = f"""
SELECT 
    YEAR(L_SHIPDATE) AS year, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue 
FROM lineitem 
WHERE 
    L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' AND 
    L_SUPPKEY IN {tuple(indian_suppliers)} AND
    L_PARTKEY IN {tuple(small_plated_copper_df['P_PARTKEY'])}
GROUP BY year
ORDER BY year
"""
revenue_results = execute_mysql_query(mysql_query_revenue)
mysql_conn.close()

# Convert to dataframe and save to CSV
revenue_df = pd.DataFrame(revenue_results, columns=['Year', 'Revenue'])
revenue_df.to_csv('query_output.csv', index=False)
