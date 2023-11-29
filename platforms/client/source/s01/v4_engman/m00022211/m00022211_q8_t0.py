import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query relevant information from MySQL (nation and region)
mysql_query = """
SELECT n.N_NATIONKEY, n.N_NAME, r.R_NAME
FROM nation n
JOIN region r ON n.N_REGIONKEY=r.R_REGIONKEY
WHERE n.N_NAME='INDIA' AND r.R_NAME='ASIA';
"""
mysql_cursor.execute(mysql_query)
india_nationkey = mysql_cursor.fetchall()

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Check if India is not within Asia or does not exist
if len(india_nationkey) == 0:
    print("No data found for INDIA within ASIA.")
    exit()

india_nationkey = india_nationkey[0][0]

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_orders = mongodb_db['orders']
mongodb_lineitem = mongodb_db['lineitem']

# Query relevant information from MongoDB
mongo_pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {
                '$in': ['1995', '1996']
            }
        }
    },
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'lineitems.L_PARTKEY': {'$eq': 'SMALL PLATED COPPER'}
        }
    },
    {
        '$project': {
            'O_ORDERKEY': 1,
            'O_ORDERDATE': 1,
            'L_EXTENDEDPRICE': '$lineitems.L_EXTENDEDPRICE',
            'L_DISCOUNT': '$lineitems.L_DISCOUNT',
        }
    }
]

orders_extended = list(mongodb_orders.aggregate(mongo_pipeline))

# Convert MongoDB result to DataFrame
orders_df = pd.DataFrame(orders_extended)

# Calculate revenue
orders_df['Revenue'] = orders_df['L_EXTENDEDPRICE'] * (1 - orders_df['L_DISCOUNT'])
orders_df['Year'] = pd.to_datetime(orders_df['O_ORDERDATE']).dt.year

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data from Redis
suppliers_df = pd.read_json(redis_conn.get('supplier'))
india_suppliers = suppliers_df[suppliers_df['S_NATIONKEY'] == india_nationkey]

# Combine results and calculate market share
total_revenue = orders_df.groupby('Year')['Revenue'].sum().reset_index()
india_revenue = (orders_df[orders_df['O_ORDERKEY'].isin(india_suppliers['S_SUPPKEY'])]
                 .groupby('Year')['Revenue'].sum()
                 .reset_index())

market_share = pd.merge(india_revenue, total_revenue, on='Year', suffixes=('_INDIA', '_TOTAL'))
market_share['Market_Share'] = market_share['Revenue_INDIA'] / market_share['Revenue_TOTAL']
market_share = market_share[['Year', 'Market_Share']]

# Output the results to CSV
market_share.to_csv('query_output.csv', index=False)
