import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from MySQL: customer
mysql_cursor.execute("SELECT * FROM customer")
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

# Get data from MongoDB: nation, supplier
nations = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
suppliers = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))

# Get data from Redis: lineitem
lineitem_dict = redis_client.get('lineitem')
lineitems = pd.DataFrame(lineitem_dict)

# Build the query
result = (
    lineitems
    .merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nations.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPPLIER_NATION'}), on='S_NATIONKEY')
    .merge(customers, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
    .merge(nations.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUSTOMER_NATION'}), on='C_NATIONKEY')
    .query("SUPPLIER_NATION in ['INDIA', 'JAPAN'] and CUSTOMER_NATION in ['INDIA', 'JAPAN'] and SUPPLIER_NATION != CUSTOMER_NATION and L_SHIPDATE >= '1995-01-01' and L_SHIPDATE <= '1996-12-31'")
    .assign(year=lambda df: pd.to_datetime(df['L_SHIPDATE']).dt.year)
    .assign(revenue=lambda df: df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']))
    .groupby(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'year'])
    .agg({'revenue': 'sum'})
    .reset_index()
    .sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'year'])
)

# Save the result to query_output.csv
result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
