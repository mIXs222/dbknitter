import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Function to get data from MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    query = """
    SELECT c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, n.N_NAME, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
    FROM customer c
    LEFT JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
    """
    df_mysql = pd.read_sql(query, connection)
    connection.close()
    return df_mysql

# Function to get data from MongoDB
def get_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    orders = db.orders.find({
        "O_ORDERDATE": {
            "$gte": datetime(1993, 10, 1),
            "$lt": datetime(1994, 1, 1)
        },
        "O_ORDERSTATUS": "R"
    }, {"O_ORDERKEY": 1, "O_CUSTKEY": 1})
    
    df_orders = pd.DataFrame(list(orders))
    if not df_orders.empty:
        df_orders.rename(columns={"O_ORDERKEY": "L_ORDERKEY", "O_CUSTKEY": "C_CUSTKEY"}, inplace=True)
    client.close()
    return df_orders

# Function to get data from Redis
def get_redis_data():
    redis = DirectRedis(host='redis', port=6379, db=0)
    lineitems = redis.get('lineitem')
    df_redis = pd.read_json(lineitems, orient='records')
    return df_redis

# Combine the data from different databases
df_mysql = get_mysql_data()
df_mongodb = get_mongodb_data()
df_redis = get_redis_data()

# Merge data frames
df_merged = pd.merge(df_mysql, df_mongodb, on='C_CUSTKEY', how='inner')
df_merged = pd.merge(df_merged, df_redis, on='L_ORDERKEY', how='inner')

# Calculate revenue lost
df_merged['REVENUE_LOST'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Group by customer with the sum of revenue lost
df_final = df_merged.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']) \
                    .agg({'REVENUE_LOST': 'sum'}) \
                    .reset_index()

# Sort as per the requirements
df_final.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                     ascending=[True, True, True, False], inplace=True)

# Write to csv
df_final.to_csv("query_output.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
