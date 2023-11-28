import pandas as pd
import pymysql
import pymongo
import direct_redis

# Function to connect to MySQL
def mysql_connect(db_name, user, password, host):
    connection = pymysql.connect(host=host, user=user, password=password, db=db_name, cursorclass=pymysql.cursors.Cursor)
    return connection

# Function to execute a MySQL query
def mysql_execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(result, columns=col_names)

# Function to connect to MongoDB
def mongo_connect(host, port, db_name):
    client = pymongo.MongoClient(host, port)
    db = client[db_name]
    return db

# Function to get data from MongoDB
def mongo_get_data(db, collection_name):
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    return data

# Connect to MySQL
mysql_conn = mysql_connect("tpch", "root", "my-secret-pw", "mysql")

# Queries for MySQL
orders_query = """
SELECT * FROM orders WHERE O_ORDERSTATUS = 'F';
"""

lineitem_query = """
SELECT * FROM lineitem WHERE L_RECEIPTDATE > L_COMMITDATE;
"""

# Get data from MySQL
df_orders = mysql_execute_query(mysql_conn, orders_query)
df_lineitem = mysql_execute_query(mysql_conn, lineitem_query)

# Connect to MongoDB and get nation data
mongo_db = mongo_connect("mongodb", 27017, "tpch")
df_nation = mongo_get_data(mongo_db, "nation")

# Connect to Redis and get supplier data
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
df_supplier_raw = r.get('supplier')
df_supplier = pd.read_msgpack(df_supplier_raw)

# Filter suppliers by nation (Saudi Arabia)
df_nation_saudi = df_nation[df_nation['N_NAME'] == 'SAUDI ARABIA']
df_supplier = df_supplier[df_supplier['S_NATIONKEY'].isin(df_nation_saudi['N_NATIONKEY'])]

# Merge dataframes to get relevant suppliers and line items
df_merge = pd.merge(df_supplier, df_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
df_merge = pd.merge(df_merge, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Define and apply subquery filter conditions
filtered_suppliers = df_merge.groupby('S_SUPPKEY').filter(lambda x: any(
    (df_merge['L_ORDERKEY'] == x['L_ORDERKEY'].iloc[0])
    & (df_merge['L_SUPPKEY'] != x['S_SUPPKEY'].iloc[0])
))
filtered_suppliers = filtered_suppliers.groupby('S_SUPPKEY').filter(lambda x: not any(
    (df_merge['L_ORDERKEY'] == x['L_ORDERKEY'].iloc[0])
    & (df_merge['L_SUPPKEY'] != x['S_SUPPKEY'].iloc[0])
    & (df_merge['L_RECEIPTDATE'] > x['L_COMMITDATE'].iloc[0])
))

# Aggregate results
final_results = filtered_suppliers.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Sort the results
final_results = final_results.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output to CSV
final_results.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
