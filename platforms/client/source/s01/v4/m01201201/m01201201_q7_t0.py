import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Function to execute the MySQL query
def fetch_mysql_data():
    conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )
    query = """
    SELECT
        S_NAME,
        S_SUPPKEY,
        N1.N_NAME AS SUPP_NATION,
        N1.N_NATIONKEY AS SUPP_NATIONKEY
    FROM
        supplier,
        nation N1
    WHERE
        S_NATIONKEY = N1.N_NATIONKEY
    """
    supplier_df = pd.read_sql(query, conn)

    query = """
    SELECT
        O_ORDERKEY,
        C_CUSTKEY,
        O_ORDERDATE
    FROM
        orders
    """
    orders_df = pd.read_sql(query, conn)
    orders_df['L_YEAR'] = orders_df['O_ORDERDATE'].apply(lambda d: d.year)
    
    conn.close()
    return supplier_df, orders_df

# Function to execute the MongoDB query
def fetch_mongo_data():
    client = pymongo.MongoClient('mongodb', 27017)
    mongo_db = client['tpch']
    lineitems = list(mongo_db.lineitem.find(
        {"$and": [{"L_SHIPDATE": {"$gte": datetime(1995, 1, 1)}},
                  {"L_SHIPDATE": {"$lte": datetime(1996, 12, 31)}}]}
    ))
    lineitem_df = pd.DataFrame(lineitems)
    client.close()
    return lineitem_df

# Function to read data from Redis and convert to DataFrame
def fetch_redis_data():
    r = direct_redis.DirectRedis(port=6379, host='redis')
    customer_df = pd.read_json(r.get('customer'), orient='index')
    customer_df = customer_df.reset_index(drop=True)
    return customer_df

# Perform the query joining
def perform_query(supplier_df, orders_df, lineitem_df, customer_df, nation_df):
    # Merging dataframes
    merged_df = supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    merged_df = merged_df.merge(orders_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    merged_df = merged_df.merge(customer_df, left_on='C_CUSTKEY', right_on='C_CUSTKEY')
    merged_df = merged_df.merge(nation_df.rename(columns={'N_NAME': 'CUST_NATION', 'N_NATIONKEY': 'C_NATIONKEY'}),
                                left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    # Filtering based on nation names
    shipping = merged_df[
        ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
        ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
    ]
    shipping['VOLUME'] = shipping['L_EXTENDEDPRICE'] * (1 - shipping['L_DISCOUNT'])
    result = shipping.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg(REVENUE=('VOLUME', 'sum')).reset_index()
    result.to_csv('query_output.csv', index=False)

# Execute the queries and perform the data join
supplier_df, orders_df = fetch_mysql_data()
lineitem_df = fetch_mongo_data()
customer_df = fetch_redis_data()
nation_df = supplier_df[['SUPP_NATION', 'SUPP_NATIONKEY']].drop_duplicates()
perform_query(supplier_df, orders_df, lineitem_df, customer_df, nation_df)
