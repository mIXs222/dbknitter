import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL and execute a query
def mysql_query(query):
    connection = pymysql.connect(host="mysql", user="root", password="my-secret-pw", db="tpch")
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df = pd.DataFrame(result, columns=columns)
    finally:
        connection.close()
    return df

# Function to connect to MongoDB and retrieve data
def mongodb_query(collection):
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]
    data = list(db[collection].find())
    df = pd.DataFrame(data)
    client.close()
    return df

# Function to connect to Redis and retrieve data as DataFrame
def redis_query(key):
    client = DirectRedis(host="redis", port=6379, db=0)
    df = pd.read_msgpack(client.get(key))
    client.close()
    return df

# MySQL queries for tables supplier and customer
supplier_query = "SELECT * FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA');"
customer_query = "SELECT * FROM customer;"

# Retrieve data from all databases
supplier_df = mysql_query(supplier_query)
customer_df = mysql_query(customer_query)
nation_df = mongodb_query("nation")
region_df = mongodb_query("region")
orders_df = redis_query('orders')
lineitem_df = redis_query('lineitem')

# Join relevant tables
IND_nations = nation_df[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].tolist()
IND_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(IND_nations)]
ASIA_region = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()
ASIA_nations = nation_df[nation_df['N_REGIONKEY'].isin(ASIA_region)]['N_NATIONKEY'].tolist()

# Filter customers and orders based on ASIA
ASIA_customers = customer_df[customer_df['C_NATIONKEY'].isin(ASIA_nations)]
ASIA_orders = orders_df[orders_df['O_CUSTKEY'].isin(ASIA_customers['C_CUSTKEY'])]

# Prepare data
lineitem_df['L_YEAR'] = pd.to_datetime(lineitem_df['L_SHIPDATE']).dt.year
relevant_lineitems = lineitem_df[(lineitem_df['L_YEAR'].isin([1995, 1996])) & (lineitem_df['L_PARTKEY'].str.contains('SMALL PLATED COPPER'))]
IND_lineitems = relevant_lineitems[relevant_lineitems['L_SUPPKEY'].isin(IND_suppliers['S_SUPPKEY'])]

# Calculate market share for 1995 and 1996
market_share = {}
for year in [1995, 1996]:
    total_revenue = relevant_lineitems[relevant_lineitems['L_YEAR'] == year]['L_EXTENDEDPRICE'].sum()
    IND_revenue = IND_lineitems[IND_lineitems['L_YEAR'] == year]['L_EXTENDEDPRICE'].sum()
    market_share[year] = IND_revenue / total_revenue if total_revenue else 0

# Write output to CSV
output_df = pd.DataFrame.from_dict(market_share, orient='index', columns=['Market Share']).reset_index()
output_df.rename(columns={'index': 'Year'}, inplace=True)
output_df.to_csv('query_output.csv', index=False)
