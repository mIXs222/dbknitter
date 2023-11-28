import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL (assuming pymysql is already installed)
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor  # Do NOT use DictCursor as per instructions
)

# Create a cursor object
cursor = mysql_connection.cursor()

# Execute query for 'lineitem' and 'region' tables from MySQL
cursor.execute("SELECT * FROM lineitem")
lineitem_data = cursor.fetchall()
df_lineitem = pd.DataFrame(list(lineitem_data), columns=[desc[0] for desc in cursor.description])

cursor.execute("SELECT * FROM region")
region_data = cursor.fetchall()
df_region = pd.DataFrame(list(region_data), columns=[desc[0] for desc in cursor.description])

# Filtering region for 'ASIA'
region_asia_key = df_region[df_region['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get 'nation' and 'supplier' collection data from MongoDB
nation_data = mongodb_db['nation'].find()
supplier_data = mongodb_db['supplier'].find()

# Convert to Pandas DataFrame
df_nation = pd.DataFrame(list(nation_data))
df_supplier = pd.DataFrame(list(supplier_data))

# Filtering nation for 'INDIA' within 'ASIA' region
nation_asia = df_nation[df_nation['N_REGIONKEY'] == region_asia_key]
nation_india = nation_asia[nation_asia['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0, decode_responses=True)

# Get 'part' and 'customer' data from Redis
df_part = pd.read_json(redis_client.get('part'))
df_customer = pd.read_json(redis_client.get('customer'))

# Filtering 'part' for 'SMALL PLATED COPPER'
part_spc = df_part[df_part['P_TYPE'] == 'SMALL PLATED COPPER']

# Preparing the data for analysis
# Joining tables: lineitem -> orders -> customer -> nation -> region
# and filtering based on conditions specified

# Assuming 'orders' collection data has been extracted from MongoDB to DataFrame
df_orders = pd.DataFrame(list(mongodb_db['orders'].find()))

# Filter orders between 1995 and 1996
df_orders['O_ORDERDATE'] = pd.to_datetime(df_orders['O_ORDERDATE'])
df_orders_filtered = df_orders[(df_orders['O_ORDERDATE'].dt.year == 1995) | (df_orders['O_ORDERDATE'].dt.year == 1996)]

# Filter lineitems with 'SMALL PLATED COPPER'
df_lineitem_filtered = df_lineitem[df_lineitem['L_PARTKEY'].isin(part_spc['P_PARTKEY'])]

# Calculate volume and join with orders
df_lineitem_filtered['volume'] = df_lineitem_filtered['L_EXTENDEDPRICE'] * (1 - df_lineitem_filtered['L_DISCOUNT'])
df_joined = df_lineitem_filtered.merge(df_orders_filtered, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filtering customers and then joining
df_customer_filtered = df_customer[df_customer['C_NATIONKEY'] == nation_india]
df_joined = df_joined.merge(df_customer_filtered, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate total volume per year
df_joined['year'] = df_joined['O_ORDERDATE'].dt.year
total_volume_per_year = df_joined.groupby('year')['volume'].sum().reset_index(name='total_volume')

# Calculate volume for 'INDIA'
india_volume_per_year = df_joined.groupby('year')['volume'].sum().reset_index(name='india_volume')

# Calculate market share
market_share = india_volume_per_year.merge(total_volume_per_year, on='year')
market_share['market_share'] = market_share['india_volume'] / market_share['total_volume']

# Order results by year
market_share = market_share.sort_values(by='year')

# Write to CSV file
market_share.to_csv('query_output.csv', index=False)

# Closing connections
cursor.close()
mysql_connection.close()
mongodb_client.close() 
redis_client.close()  # Assuming DirectRedis 'close' method exists or `__exit__` context management is properly implemented
