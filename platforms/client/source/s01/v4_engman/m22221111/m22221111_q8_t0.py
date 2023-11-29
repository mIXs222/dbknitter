# query.py
import pymongo
import pandas as pd
import direct_redis

# Function to calculate the market share for a given year
def calculate_market_share(year, lineitem_df, order_df, supplier_df, nation_df, region_df):
    # Filter orders by year and join with lineitem on orderkey
    orders_filtered = order_df[order_df['O_ORDERDATE'].dt.year == year]
    lineorder_df = pd.merge(orders_filtered, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Join lineorder with supplier on suppkey
    linesupplier_df = pd.merge(lineorder_df, supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

    # Join linesupplier with nation on nationkey
    linesuppliernation_df = pd.merge(linesupplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Join linesuppliernation with region on regionkey
    complete_df = pd.merge(linesuppliernation_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

    # Filter for ASIA and SMALL PLATED COPPER, calculate revenue for INDIA and all ASIA
    asia_df = complete_df[complete_df['R_NAME'] == 'ASIA']
    asia_small_plated_copper_df = asia_df[asia_df['P_TYPE'] == 'SMALL PLATED COPPER']
    revenue_india = asia_small_plated_copper_df[asia_small_plated_copper_df['N_NAME'] == 'INDIA']
    revenue_india_sum = (revenue_india['L_EXTENDEDPRICE'] * (1 - revenue_india['L_DISCOUNT'])).sum()
    revenue_asia_sum = (asia_small_plated_copper_df['L_EXTENDEDPRICE'] * (1 - asia_small_plated_copper_df['L_DISCOUNT'])).sum()

    # Calculate market share for India within Asia
    market_share_india = revenue_india_sum / revenue_asia_sum if revenue_asia_sum else 0

    return year, market_share_india

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client['tpch']
customer_mongo = mongodb['customer']
orders_mongo = mongodb['orders']
lineitem_mongo = mongodb['lineitem']

# Convert MongoDB collections to pandas DataFrames
customer_df = pd.DataFrame(list(customer_mongo.find()))
orders_df = pd.DataFrame(list(orders_mongo.find()))
lineitem_df = pd.DataFrame(list(lineitem_mongo.find()))

# Fix the date formats in orders
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Convert Redis tables to pandas DataFrames
nation_df = pd.read_json(r.get('nation'))
region_df = pd.read_json(r.get('region'))
part_df = pd.read_json(r.get('part'))
supplier_df = pd.read_json(r.get('supplier'))

# Calculate market share for 1995 and 1996
market_share_1995 = calculate_market_share(1995, lineitem_df, orders_df, supplier_df, nation_df, region_df)
market_share_1996 = calculate_market_share(1996, lineitem_df, orders_df, supplier_df, nation_df, region_df)

# Turn the results into a DataFrame and write to CSV
results_df = pd.DataFrame([market_share_1995, market_share_1996], columns=['Order Year', 'Market Share'])
results_df.to_csv('query_output.csv', index=False)
