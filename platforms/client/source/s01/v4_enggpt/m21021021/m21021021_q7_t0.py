import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load data from MySQL
with mysql_conn.cursor() as cursor:
    query = """
    SELECT c.C_NAME, c.C_CUSTKEY, n.N_NAME AS C_NATION, c.C_ACCTBAL
    FROM customer AS c
    JOIN nation AS n ON c.C_NATIONKEY = n.N_NATIONKEY
    WHERE n.N_NAME IN ('JAPAN', 'INDIA')
    """
    cursor.execute(query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_NAME', 'C_CUSTKEY', 'C_NATION', 'C_ACCTBAL'])

mysql_conn.close()

# Load data from MongoDB
lineitems = pd.DataFrame(list(mongodb_db['lineitem'].find(
    {
        "L_SHIPDATE": {
            "$gte": "1995-01-01",
            "$lte": "1996-12-31"
        }
    },
    {
        "L_ORDERKEY": 1,
        "L_EXTENDEDPRICE": 1,
        "L_DISCOUNT": 1,
        "L_SHIPDATE": 1
    }
)))

# Prepare Redis keys
nation_keys = [f"nation:{i}" for i in range(25)]
supplier_keys = [f"supplier:{i}" for i in range(10000)]
orders_keys = [f"orders:{i}" for i in range(60000)]

# Load data from Redis
nations = pd.DataFrame([redis_conn.get(k) for k in nation_keys if redis_conn.get(k) is not None])
suppliers = pd.DataFrame([redis_conn.get(k) for k in supplier_keys if redis_conn.get(k) is not None])
orders = pd.DataFrame([redis_conn.get(k) for k in orders_keys if redis_conn.get(k) is not None])

# Convert JSON strings to dictionaries and then to DataFrame
nations = pd.DataFrame.from_records(nations[0].apply(eval))
suppliers = pd.DataFrame.from_records(suppliers[0].apply(eval))
orders = pd.DataFrame.from_records(orders[0].apply(eval))

# Merge dataframes to create the report
report = (
    orders.merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(suppliers, left_on='O_ORDERKEY', right_on='S_SUPPKEY')
    .merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

report['REVENUE'] = report['L_EXTENDEDPRICE'] * (1 - report['L_DISCOUNT'])
report = (
    report.loc[(report['C_NATION'].isin(['JAPAN', 'INDIA'])) 
               & (report['N_NAME'].isin(['JAPAN', 'INDIA']))]
)

report['YEAR'] = pd.to_datetime(report['L_SHIPDATE']).dt.year
final_report = report.groupby(['N_NAME', 'C_NATION', 'YEAR'])\
                     .agg({'REVENUE': 'sum'}).reset_index()

final_report = final_report.sort_values(by=['N_NAME', 'C_NATION', 'YEAR'])

# Write to CSV
final_report.to_csv('query_output.csv', index=False)
