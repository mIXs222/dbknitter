import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Querying MySQL
mysql_query = """
SELECT 
    C_CUSTKEY, C_NATIONKEY,
    S_SUPPKEY, S_NATIONKEY,
    N_NATIONKEY, N_NAME,
    R_REGIONKEY, R_NAME
FROM 
    customer INNER JOIN supplier ON C_NATIONKEY = S_NATIONKEY
    INNER JOIN nation ON S_NATIONKEY = N_NATIONKEY
    INNER JOIN region ON N_REGIONKEY = R_REGIONKEY
WHERE
    R_NAME = 'ASIA'
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

mysql_conn.close()

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Querying MongoDB
orders_df = pd.DataFrame(
    mongo_db.orders.find(
        {
            "O_ORDERDATE": {
                "$gte": "1990-01-01",
                "$lt": "1995-01-01"
            }
        },
        {
            "_id": 0, 
            "O_CUSTKEY": 1, 
            "O_ORDERKEY": 1
        }
    )
)

lineitem_df = pd.DataFrame(
    mongo_db.lineitem.find(
        {},
        {
            "_id": 0, 
            "L_ORDERKEY": 1, 
            "L_EXTENDEDPRICE": 1, 
            "L_DISCOUNT": 1, 
            "L_SUPPKEY": 1
        }
    )
)

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Loading data from Redis
nation_df = pd.read_csv(r.get('nation'), index_col=0)
region_df = pd.read_csv(r.get('region'), index_col=0)

# Combine dataframes
combined_df = (
    mysql_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on=['S_SUPPKEY', 'O_ORDERKEY'], right_on=['L_SUPPKEY', 'L_ORDERKEY'])
    .merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_df, on='R_REGIONKEY')
)

# Perform the calculation
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
grouped_df = combined_df.groupby('N_NAME', as_index=False).agg({'REVENUE': 'sum'})
result_df = grouped_df.sort_values(by='REVENUE', ascending=False)

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
