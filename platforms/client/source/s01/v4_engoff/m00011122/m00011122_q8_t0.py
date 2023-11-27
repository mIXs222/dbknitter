import pandas as pd
import pymongo
import pymysql
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Function to execute SQL query in MySQL
def execute_sql(query):
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [col[0] for col in cursor.description]
    return pd.DataFrame(result, columns=column_names)

# Function to get MongoDB collection as DataFrame
def get_mongo_collection_as_df(collection_name):
    collection = mongodb_db[collection_name]
    data = list(collection.find({}))
    return pd.DataFrame(data)

# Function to get Redis data as DataFrame
def get_redis_data_as_df(key):
    data = redis_client.get(key)
    return pd.read_json(data)

# Retrieve data from MySQL
sql_query_nation = "SELECT * FROM nation;"
sql_query_region = "SELECT * FROM region;"
sql_query_part = "SELECT * FROM part;"

nation_df = execute_sql(sql_query_nation)
region_df = execute_sql(sql_query_region)
part_df = execute_sql(sql_query_part)

# Retrieve data from MongoDB
supplier_df = get_mongo_collection_as_df('supplier')

# Retrieve data from Redis
orders_df = get_redis_data_as_df('orders')
lineitem_df = get_redis_data_as_df('lineitem')

# Preprocessing and query logic
# Merge and filter datasets
asia_region = region_df[region_df['R_NAME'] == 'ASIA']
india_nations = nation_df[nation_df['N_NAME'] == 'INDA']
india_nations = india_nations.merge(asia_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
india_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(india_nations['N_NATIONKEY'])]
small_plated_copper_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Calculate the revenue from line items
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Define the years of interest
years = [1995, 1996]

# Function to calculate market share
def calculate_market_share(year):
    orders_filtered = orders_df[orders_df['O_ORDERDATE'].str.contains(str(year))]
    relevant_lineitems = lineitem_df[lineitem_df['L_ORDERKEY'].isin(orders_filtered['O_ORDERKEY'])]
    relevant_lineitems = relevant_lineitems.merge(small_plated_copper_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
    india_revenue = relevant_lineitems[relevant_lineitems['L_SUPPKEY'].isin(india_suppliers['S_SUPPKEY'])]['revenue'].sum()
    total_revenue = relevant_lineitems['revenue'].sum()
    return india_revenue / total_revenue if total_revenue else 0

# Calculate market share for the specified years
market_shares = [calculate_market_share(year) for year in years]

# Storing the result to CSV file
output_df = pd.DataFrame(market_shares, index=years, columns=['MarketShare'])
output_df.to_csv('query_output.csv', header=False)

# Closing connections
mysql_conn.close()
mongodb_client.close()
