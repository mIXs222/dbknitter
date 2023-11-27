import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis

# Connect to MySQL database.
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB.
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis directly for pandas.
redis = DirectRedis(host='redis', port=6379, db=0)

# SQL Query for MySQL.
mysql_query = """
SELECT
    DATE_FORMAT(O_ORDERDATE, '%%Y') AS O_YEAR,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME
FROM
    lineitem
JOIN
    orders ON L_ORDERKEY = O_ORDERKEY
WHERE
    O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

# Load data from MySQL.
df_mysql = pd.read_sql(mysql_query, mysql_conn)

# Load data from MongoDB: part, customer.
df_part = pd.DataFrame(list(mongo_db['part'].find({'P_TYPE': 'SMALL PLATED COPPER'})))
df_customer = pd.DataFrame(list(mongo_db['customer'].find()))

# Load data from Redis: nation, supplier.
# Working with Redis structure may require additional code adjustments since it does not store data in tabular form by default.
df_nation = pd.read_json(redis.get('nation'), typ='series')
df_supplier = pd.read_json(redis.get('supplier'), typ='series')

# Convert Redis data into DataFrame if required.
# Assumption: Redis data is stored in JSON-like format, which can be directly converted to DataFrame.
# For simplicity, let's assume the data is already in the correct DataFrame format.

# Now we combine the data using pandas operations.
# First, we join data from MySQL and Redis which represents the join of lineitem, orders, and supplier.
df_join1 = pd.merge(df_mysql, df_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Join the MongoDB part data.
df_join2 = pd.merge(df_join1, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Join MongoDB customer data.
df_join3 = pd.merge(df_join2, df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Lastly, join the Redis nation data twice for both nation columns.
df_join4 = pd.merge(df_join3, df_nation.rename(columns=lambda c: f"N1_{c}"), left_on='C_NATIONKEY', right_on='N1_N_NATIONKEY')
df_final_join = pd.merge(df_join4, df_nation.rename(columns=lambda c: f"N2_{c}"), left_on='S_NATIONKEY', right_on='N2_N_NATIONKEY')

# Filter on ASIA region and calculate the market share.
df_filtered = df_final_join[df_final_join['N1_R_NAME'] == 'ASIA']
df_filtered['NATION'] = df_filtered['N2_N_NAME']
df_result = df_filtered.groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': x[x['NATION'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()
    })
).reset_index()

# Output the result to CSV.
df_result.to_csv('query_output.csv', index=False)

# Clean up connections.
mysql_conn.close()
mongo_client.close()
