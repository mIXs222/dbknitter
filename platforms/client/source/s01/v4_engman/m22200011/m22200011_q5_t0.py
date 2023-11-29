# query.py
import pymysql
import pandas as pd
import pymongo
from direct_redis import DirectRedis

# Connection to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB database
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connection to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Querying MySQL for suppliers and customers from Asia
mysql_cursor.execute("""
SELECT 
    s.S_SUPPKEY, c.C_CUSTKEY, n.N_NAME
FROM 
    supplier s 
JOIN 
    nation n ON s.S_NATIONKEY = n.N_NATIONKEY 
JOIN 
    region r ON n.N_REGIONKEY = r.R_REGIONKEY
JOIN
    customer c ON s.S_NATIONKEY = c.C_NATIONKEY
WHERE
    r.R_NAME = 'ASIA';
""")
suppliers_customers_from_asia = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'C_CUSTKEY', 'N_NAME'])

# Querying Redis for nation and region
nations_pd = pd.read_json(redis_client.get('nation'))
regions_pd = pd.read_json(redis_client.get('region'))

# Filtering nations and regions for ASIA
asia_nations_keys = regions_pd[regions_pd['R_NAME'] == 'ASIA']['R_REGIONKEY'].unique()
asia_nations = nations_pd[nations_pd['N_REGIONKEY'].isin(asia_nations_keys)]

# Querying MongoDB orders
orders_df = pd.DataFrame(list(mongodb_db.orders.find(
    {'O_ORDERDATE': {'$gte': pd.Timestamp('1990-01-01'), '$lt': pd.Timestamp('1995-01-01')}},
)))

# Querying MongoDB lineitem
lineitem_df = pd.DataFrame(list(mongodb_db.lineitem.find()))

# Filtering lineitems by qualifying orders and joining with the supplier and customer data
filtered_lineitems = lineitem_df[lineitem_df['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'])]
filtered_lineitems = filtered_lineitems.merge(suppliers_customers_from_asia, left_on=['L_SUPPKEY'], right_on=['S_SUPPKEY'])
filtered_lineitems = filtered_lineitems.merge(orders_df[['O_ORDERKEY', 'O_CUSTKEY']], on='O_ORDERKEY')
filtered_lineitems = filtered_lineitems[filtered_lineitems['C_CUSTKEY'].notnull()]

# Calculating revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
revenue_by_nation = filtered_lineitems.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sorting by revenue in descending order
revenue_by_nation.sort_values(by=['REVENUE'], ascending=False, inplace=True)

# Write to CSV file
revenue_by_nation.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_client.close()
