import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Define connection parameters
mysql_conn_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
    "charset": "utf8mb4",
}

# Establish connection to MySQL
try:
    mysql_conn = pymysql.connect(**mysql_conn_params)
except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL Platform: {e}")
    exit(1)

# Query part and region tables from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT s.S_SUPPKEY, s.S_NATIONKEY
        FROM supplier AS s
    """)
    supplier_df = pd.DataFrame(cursor.fetchall(), columns=["S_SUPPKEY", "S_NATIONKEY"])

    cursor.execute("""
        SELECT c.C_CUSTKEY, c.C_NATIONKEY
        FROM customer AS c
    """)
    customer_df = pd.DataFrame(cursor.fetchall(), columns=["C_CUSTKEY", "C_NATIONKEY"])

    cursor.execute("""
        SELECT r.R_REGIONKEY, r.R_NAME
        FROM region AS r
        WHERE r.R_NAME = 'ASIA'
    """)
    region_df = pd.DataFrame(cursor.fetchall(), columns=["R_REGIONKEY", "R_NAME"])

# Close MySQL connection
mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query Redis tables and convert to Pandas DataFrames
nation_df = pd.read_json(redis_conn.get('nation'))
part_df = pd.read_json(redis_conn.get('part'))
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter for nation INDIA in ASIA region
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(region_df['R_REGIONKEY'])]
india_nationkey = asia_nations.loc[asia_nations['N_NAME'] == 'INDIA', 'N_NATIONKEY'].iloc[0]

# Filter part data
part_df = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Filter orders by year
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'].dt.year >= 1995) & (orders_df['O_ORDERDATE'].dt.year <= 1996)]

# Join DataFrames to get the lineitem volumes related to INDIA and part type
result_df = (
    lineitem_df
    .merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
)

# Calculate volume as extended price * (1 - discount)
result_df['VOLUME'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Group by year and calculate INDIA market share
result_df = result_df[result_df['C_NATIONKEY'] == india_nationkey]
market_share_df = result_df.groupby(result_df['O_ORDERDATE'].dt.year)['VOLUME'].sum().reset_index()
total_volume = result_df['VOLUME'].sum()
market_share_df['MARKET_SHARE'] = market_share_df['VOLUME'] / total_volume

# Order results by year and save to CSV
market_share_df.sort_values(by='O_ORDERDATE', inplace=True)
market_share_df.to_csv('query_output.csv', index=False)
