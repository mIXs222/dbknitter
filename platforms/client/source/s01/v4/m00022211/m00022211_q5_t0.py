import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT N_NATIONKEY, N_NAME FROM nation
        WHERE N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA')
    """)
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

    cursor.execute("""
        SELECT S_SUPPKEY, S_NATIONKEY FROM supplier
    """)
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])

    mysql_conn.close()

# Query MongoDB
orders = pd.DataFrame(list(mongodb.orders.find({
    'O_ORDERDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
}, {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1})))

lineitems = pd.DataFrame(list(mongodb.lineitem.find({}, {
    '_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SUPPKEY': 1
})))

# Query Redis
supplier_df = pd.read_json(redis.get('supplier'))
customer_df = pd.read_json(redis.get('customer'))

# Filter out the data based on the nation keys from MySql
suppliers_filtered = suppliers[suppliers['S_NATIONKEY'].isin(nations['N_NATIONKEY'])]
supplier_df_filtered = supplier_df[supplier_df['S_SUPPKEY'].isin(suppliers_filtered['S_SUPPKEY'])]
customer_df_filtered = customer_df[customer_df['C_NATIONKEY'].isin(nations['N_NATIONKEY'])]

# Merge the dataframes to perform the SQL-like operations
merged_df = pd.merge(customer_df_filtered, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, supplier_df_filtered, left_on=['S_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
merged_df = pd.merge(merged_df, nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Compute the revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
results = merged_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Order by revenue
results = results.sort_values(by='REVENUE', ascending=False)

# Write to CSV
results.to_csv('query_output.csv', index=False)
