import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to calculate the market share for given years.
def calculate_market_share(years_data, india_suppliers):
    market_shares = []
    for year in years_data:
        filtered_data = years_data[year]
        # Filter for only small plated copper from suppliers in India
        india_data = filtered_data[filtered_data['S_SUPPKEY'].isin(india_suppliers)]
        total_revenue = (filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])).sum()
        india_revenue = (india_data['L_EXTENDEDPRICE'] * (1 - india_data['L_DISCOUNT'])).sum()
        market_share = (india_revenue / total_revenue) if total_revenue else 0
        market_shares.append((year, market_share))
    return market_shares

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
supplier_sql = "SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA')"
suppliers = pd.read_sql(supplier_sql, mysql_conn)
india_suppliers = suppliers['S_SUPPKEY'].tolist()
mysql_conn.close()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem = pd.DataFrame(list(mongodb_db['lineitem'].find()))

# Connect to Redis
redis_client = DirectRedis(port=6379, db=0, host='redis')
customer_df = pd.DataFrame(redis_client.get('customer'))  # Convert to DataFrame as needed

# Now let's filter the lineitem data for the years 1995 and 1996, product type SMALL PLATED COPPER
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
years_data = {
    '1995': lineitem[(lineitem['L_SHIPDATE'].dt.year == 1995)],
    '1996': lineitem[(lineitem['L_SHIPDATE'].dt.year == 1996)]
}

# Calculate market share for India within Asia for 1995 and 1996
market_shares = calculate_market_share(years_data, india_suppliers)

# Write to CSV
market_shares_df = pd.DataFrame(market_shares, columns=['Order Year', 'Market Share'])
market_shares_df.to_csv('query_output.csv', index=False)
