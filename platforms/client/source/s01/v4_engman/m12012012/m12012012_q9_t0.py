import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime


def get_mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4'
    )


def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']


def get_redis_connection():
    return DirectRedis(host='redis', port=6379, db=0)


def run_query():
    conn_mysql = get_mysql_connection()
    conn_mongo = get_mongodb_connection()
    conn_redis = get_redis_connection()

    part_df = pd.read_sql("SELECT * FROM part WHERE P_NAME LIKE '%dim%'", conn_mysql)
    nation_df = pd.DataFrame(list(conn_mongo.nation.find()))
    supplier_df = pd.DataFrame(list(conn_mongo.supplier.find()))

    partsupp_df = conn_redis.get('partsupp')
    lineitem_df = conn_redis.get('lineitem')

    conn_mysql.close()

    if isinstance(partsupp_df, bytes):
        partsupp_df = pd.read_msgpack(partsupp_df)

    if isinstance(lineitem_df, bytes):
        lineitem_df = pd.read_msgpack(lineitem_df)

    # Combining the tables
    combined_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    combined_df = pd.merge(combined_df, partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    combined_df = pd.merge(combined_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    combined_df = pd.merge(combined_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Calculating profit
    combined_df['profit'] = (combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])) - (combined_df['PS_SUPPLYCOST'] * combined_df['L_QUANTITY'])
    
    # Extracting the year from the L_SHIPDATE
    combined_df['year'] = pd.to_datetime(combined_df['L_SHIPDATE']).dt.year

    # Group by nation and year
    result = combined_df.groupby(['N_NAME', 'year']).agg({'profit': 'sum'}).reset_index()

    # Sorting the result as per the requirement
    result = result.sort_values(['N_NAME', 'year'], ascending=[True, False])

    result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    run_query()
