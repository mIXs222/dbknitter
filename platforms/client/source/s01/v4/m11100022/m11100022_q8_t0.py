# query.py
import pymysql
import pymongo
import pandas as pd

# Function to execute a SQL query and return a pandas DataFrame
def execute_sql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        df.columns = [col[0] for col in cursor.description]
    return df

# Function to execute a MongoDB query and return a pandas DataFrame
def execute_mongo_query(collection, query):
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))
    return df

# Establish connection to MySQL database
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Establish connection to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Load data from MySQL and MongoDB
df_supplier = execute_sql_query(conn, "SELECT * FROM supplier")
df_customer = execute_sql_query(conn, "SELECT * FROM customer")

df_nation = execute_mongo_query(mongo_db['nation'], {})
df_region = execute_mongo_query(mongo_db['region'], {})
df_part = execute_mongo_query(mongo_db['part'], {})
mongo_client.close()

conn.close()

# Importing DirectRedis for Redis connection
from direct_redis import DirectRedis

# Establish a connection to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Define function to load data from Redis into a DataFrame
def load_redis_table(redis_client, table_name):
    data = redis_client.get(table_name)
    # Assuming the table data is stored as a CSV string
    df = pd.read_csv(pd.compat.StringIO(str(data)))
    return df

# Load data from Redis
df_orders = load_redis_table(redis_client, 'orders')
df_lineitem = load_redis_table(redis_client, 'lineitem')

# Close the Redis connection
redis_client.close()

# Merge DataFrames
df_merged = pd.merge(df_lineitem, df_part, how='left', left_on='L_PARTKEY', right_on='P_PARTKEY')
df_merged = pd.merge(df_merged, df_supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_merged = pd.merge(df_merged, df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_merged = pd.merge(df_merged, df_customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df_merged = pd.merge(df_merged, df_nation.rename(columns={'N_NAME': 'N2_NAME', 'N_NATIONKEY': 'N2_NATIONKEY'}), how='inner', left_on='S_NATIONKEY', right_on='N2_NATIONKEY')
df_merged = pd.merge(df_merged, df_nation.rename(columns={'N_NAME': 'N1_NAME', 'N_NATIONKEY': 'N1_NATIONKEY', 'N_REGIONKEY': 'N1_REGIONKEY'}), how='inner', left_on='C_NATIONKEY', right_on='N1_NATIONKEY')
df_merged = pd.merge(df_merged, df_region, how='inner', left_on='N1_REGIONKEY', right_on='R_REGIONKEY')

# Filter the merged DataFrame based on the SQL conditions
df_filtered = df_merged[(df_merged['R_NAME'] == 'ASIA') &
                        (df_merged['O_ORDERDATE'] >= '1995-01-01') &
                        (df_merged['O_ORDERDATE'] <= '1996-12-31') &
                        (df_merged['P_TYPE'] == 'SMALL PLATED COPPER')]

# Perform aggregation to calculate market share
df_filtered['VOLUME'] = df_filtered['L_EXTENDEDPRICE'] * (1 - df_filtered['L_DISCOUNT'])
df_filtered['O_YEAR'] = pd.to_datetime(df_filtered['O_ORDERDATE']).dt.year.astype(str)

mkt_share = df_filtered.groupby('O_YEAR').apply(lambda x: pd.Series({
    'MKT_SHARE': x[x['N2_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()
})).reset_index()

# Write the result to a CSV file
mkt_share.to_csv('query_output.csv', index=False)
