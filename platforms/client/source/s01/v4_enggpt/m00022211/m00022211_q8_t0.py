import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL connection and data retrieval
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute(
        "SELECT p.P_PARTKEY, n.N_NATIONKEY, p.P_TYPE FROM part p "
        "JOIN nation n ON p.P_SIZE = n.N_REGIONKEY "
        "WHERE p.P_TYPE = 'SMALL PLATED COPPER'")
    part_nation_data = cursor.fetchall()

# Construct a DataFrame for part and nation
part_nation_df = pd.DataFrame(part_nation_data, columns=['P_PARTKEY', 'N_NATIONKEY', 'P_TYPE'])

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']

orders_data = orders_collection.find({
    "O_ORDERDATE": {
        "$gte": "1995-01-01",
        "$lte": "1996-12-31"
    }
})

orders_df = pd.DataFrame(list(orders_data))

lineitem_data = lineitem_collection.find()
lineitem_df = pd.DataFrame(list(lineitem_data))

# Redis connection and data retrieval
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_client.get('supplier')
customer_data = redis_client.get('customer')

supplier_df = pd.read_json(supplier_data)
customer_df = pd.read_json(customer_data)

# Merge the retrieved data
merged_df = part_nation_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter for the 'ASIA' region, part type 'SMALL PLATED COPPER', and the nation 'INDIA'
asian_market_df = merged_df[
    (merged_df['N_NATIONKEY'] == 'INDIA') &
    (merged_df['P_TYPE'] == 'SMALL PLATED COPPER') &
    (merged_df['O_ORDERSTATUS'] == 'ASIA')
]

# Calculate volume as the extended price adjusted for discounts
asian_market_df['VOLUME'] = asian_market_df['L_EXTENDEDPRICE'] * (1 - asian_market_df['L_DISCOUNT'])

# Extract year from O_ORDERDATE
asian_market_df['YEAR'] = pd.to_datetime(asian_market_df['O_ORDERDATE']).dt.year

# Group by YEAR
grouped_market_df = asian_market_df.groupby('YEAR')['VOLUME'].sum()

# Calculate total volume per year for normalization
total_volume_per_year = asian_market_df.groupby('YEAR')['VOLUME'].sum()

# Calculate market share
market_share = grouped_market_df / total_volume_per_year

# Prepare the final Dataframe
final_df = pd.DataFrame({'YEAR': market_share.index, 'MARKET_SHARE': market_share.values})

# Write to CSV
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
