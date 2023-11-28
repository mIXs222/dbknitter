# Import necessary libraries
import pymongo
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Define queries for MySQL and call functions to execute them
def get_parts_and_customers():
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT * FROM part")
        parts_df = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
        cursor.execute("SELECT * FROM customer")
        customers_df = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])
    return parts_df, customers_df

# Get data from MongoDB
region_df = pd.DataFrame(mongo_db.region.find(), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])
lineitem_df = pd.DataFrame(mongo_db.lineitem.find(), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Get data from Redis
nation_str = redis_client.get('nation')
supplier_str = redis_client.get('supplier')
orders_str = redis_client.get('orders')

nation_df = pd.read_json(nation_str)
supplier_df = pd.read_json(supplier_str)
orders_df = pd.read_json(orders_str)

# Close MySQL connection
mysql_connection.close()

# Function to merge data and perform analysis
def calculate_market_share(parts_df, customers_df, region_df, lineitem_df, nation_df, supplier_df, orders_df):
    # Filter data according to conditions
    asia_region_keys = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY']
    india_nation_keys = nation_df[(nation_df['N_NAME'] == 'INDIA') & (nation_df['N_REGIONKEY'].isin(asia_region_keys))]['N_NATIONKEY']

    small_plated_copper_parts = parts_df[parts_df['P_TYPE'] == 'SMALL PLATED COPPER']['P_PARTKEY']
    india_customers = customers_df[customers_df['C_NATIONKEY'].isin(india_nation_keys)]['C_CUSTKEY']
    india_orders = orders_df[(orders_df['O_CUSTKEY'].isin(india_customers)) & (orders_df['O_ORDERDATE'].str.contains('1995') | orders_df['O_ORDERDATE'].str.contains('1996'))]
    india_lineitem = lineitem_df[(lineitem_df['L_PARTKEY'].isin(small_plated_copper_parts)) & (lineitem_df['L_ORDERKEY'].isin(india_orders['O_ORDERKEY']))]
    india_lineitem['VOLUME'] = india_lineitem['L_EXTENDEDPRICE'] * (1 - india_lineitem['L_DISCOUNT'])

    # Calculate total volume per year
    india_orders['YEAR'] = pd.to_datetime(india_orders['O_ORDERDATE']).dt.year
    india_lineitem = india_lineitem.merge(india_orders[['O_ORDERKEY', 'YEAR']], on='O_ORDERKEY', how='left')

    total_volume_per_year = india_lineitem.groupby('YEAR')['VOLUME'].sum().reset_index()
    total_market_volume = india_lineitem['VOLUME'].sum()

    # Calculate market share
    total_volume_per_year['MARKET_SHARE'] = total_volume_per_year['VOLUME'] / total_market_volume

    # Sort by year
    total_volume_per_year.sort_values('YEAR', inplace=True)

    # Output to CSV
    total_volume_per_year.to_csv('query_output.csv', index=False)

# Merge data and call the function to perform analysis
parts_df, customers_df = get_parts_and_customers()
calculate_market_share(parts_df, customers_df, region_df, lineitem_df, nation_df, supplier_df, orders_df)
