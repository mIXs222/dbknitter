import pymysql.cursors
import pymongo
import direct_redis
import pandas as pd

#MySQL connection
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

#MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db_mongo = client["mydatabase"]

#Redis connection
r = direct_redis.connect_to_redis('localhost')

#Fetching data from MySQL
with conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation, region, part, supplier, customer, orders, lineitem")
    result_mysql = cursor.fetchall()
df_mysql = pd.DataFrame(result_mysql)

#Fetching data from MongoDB
df_mongodb = pd.DataFrame(list(db_mongo.nation.find()))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.region.find())))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.part.find())))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.supplier.find())))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.customer.find())))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.orders.find())))
df_mongodb = df_mongodb.append(pd.DataFrame(list(db_mongo.lineitem.find())))

#Fetching data from Redis
df_redis = r.get('nation')
df_redis = df_redis.append(r.get('region'))
df_redis = df_redis.append(r.get('part'))
df_redis = df_redis.append(r.get('supplier'))
df_redis = df_redis.append(r.get('customer'))
df_redis = df_redis.append(r.get('orders'))
df_redis = df_redis.append(r.get('lineitem'))

#Concatenate all data from different sources
df = pd.concat([df_mysql, df_mongodb, df_redis])

#SQL query in pandas
result = pd.read_sql_query(
    """SELECT
        O_YEAR,
        SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
    FROM
        (
        SELECT
            strftime('%Y', O_ORDERDATE) AS O_YEAR,
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
            N2.N_NAME AS NATION
        FROM
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        WHERE
            P_PARTKEY = L_PARTKEY
            AND S_SUPPKEY = L_SUPPKEY
            AND L_ORDERKEY = O_ORDERKEY
            AND O_CUSTKEY = C_CUSTKEY
            AND C_NATIONKEY = N1.N_NATIONKEY
            AND N1.N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'ASIA'
            AND S_NATIONKEY = N2.N_NATIONKEY
            AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
            AND P_TYPE = 'SMALL PLATED COPPER'
        ) AS ALL_NATIONS
    GROUP BY
        O_YEAR
    ORDER BY
        O_YEAR""", con=conn)

# write to csv
result.to_csv('query_output.csv', index=False)
