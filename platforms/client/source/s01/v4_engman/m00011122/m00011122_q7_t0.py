import pandas as pd
import pymysql
import pymongo
import direct_redis

# MySQL connection settings
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# MongoDB connection settings
mongodb_connection_info = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch'
}

# Redis connection settings
redis_connection_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connecting to MySQL
mysql_conn = pymysql.connect(**mysql_connection_info)
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN')")
    nation_results = cursor.fetchall()
nation_df = pd.DataFrame(nation_results, columns=['N_NATIONKEY', 'N_NAME'])

# Connecting to MongoDB
mongo_client = pymongo.MongoClient(**{k: v for k, v in mongodb_connection_info.items() if k != 'db'})
mongo_db = mongo_client[mongodb_connection_info['db']]
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
customer_df = pd.DataFrame(list(mongo_db.customer.find({}, {'_id': 0})))

# Filter suppliers and customers for 'INDIA' and 'JAPAN'
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]
customer_df = customer_df[customer_df['C_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

# Connecting to Redis
redis_conn = direct_redis.DirectRedis(host=redis_connection_info['host'], port=redis_connection_info['port'], db=redis_connection_info['db'])
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merging dataframes
lineitem_orders = pd.merge(lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
supplier_customer_nation = pd.merge(supplier_df, customer_df, left_on='S_NATIONKEY', right_on='C_NATIONKEY', suffixes=('_SUPP', '_CUST'))

# Filtering years 1995 and 1996 from order date
lineitem_orders = lineitem_orders[lineitem_orders['O_ORDERDATE'].str.contains('1995|1996')]

# Calculate revenue
lineitem_orders['REVENUE'] = lineitem_orders['L_EXTENDEDPRICE'] * (1 - lineitem_orders['L_DISCOUNT'])

# Initialize final DataFrame with desired columns
final_columns = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']
final_df = pd.DataFrame(columns=final_columns)

# Loop over supplier and customer combinations to construct the final DataFrame
for index, row in supplier_customer_nation.iterrows():
    temp_df = lineitem_orders[
        (lineitem_orders['L_SUPPKEY'] == row['S_SUPPKEY']) &
        (lineitem_orders['O_CUSTKEY'] == row['C_CUSTKEY'])
    ]
    temp_df['CUST_NATION'] = row['C_NAME']
    temp_df['SUPP_NATION'] = row['S_NAME']
    temp_df['L_YEAR'] = pd.to_datetime(temp_df['O_ORDERDATE']).dt.year
    temp_df = temp_df.groupby(['CUST_NATION', 'L_YEAR', 'SUPP_NATION']).agg({'REVENUE': 'sum'}).reset_index()
    final_df = pd.concat([final_df, temp_df])

# Reordering the columns
final_df = final_df[final_columns]

# Writing to a CSV file
final_df.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).to_csv('query_output.csv', index=False)
