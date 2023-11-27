# query.py

import pymongo
import direct_redis
import pandas as pd
import csv
from datetime import datetime

# Function to convert revenue data to proper format for market share calculation.
def calculate_market_share(year_revenues, total_revenues):
    shares = []
    for year in sorted(year_revenues):
        if total_revenues != 0:
            share = year_revenues[year] / total_revenues
        else:
            share = 0
        shares.append(share)
    return shares

def main():
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient('mongodb', 27017)
    db = mongo_client.tpch
    
    # MongoDB collections to pandas dataframes
    suppliers = pd.DataFrame(list(db.supplier.find({"S_NATIONKEY": "INDA"})))
    lineitems = pd.DataFrame(list(db.lineitem.find({"L_SHIPDATE": {'$regex': '^(199[56]).*'}})))
    
    # Establish Redis connection
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Redis dataframes
    nations_df = pd.DataFrame(redis_client.hgetall('nation'))
    parts_df = pd.DataFrame(redis_client.hgetall('part'))
    orders_df = pd.DataFrame(redis_client.hgetall('orders'))
    
    # Filter parts for 'SMALL PLATED COPPER' and convert Redis hash to DataFrame
    parts = pd.read_json(parts_df.loc['part'].values[0])  # Assuming part is stored as a JSON string
    parts = parts[parts['P_TYPE'] == 'SMALL PLATED COPPER']
    
    # Join lineitems to parts and further to suppliers on their keys
    line_parts_suppliers = pd.merge(lineitems, parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
    # Note: Considering 'S_SUPPKEY' as 's_suppkey'
    line_parts_suppliers = pd.merge(line_parts_suppliers, suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    
    # Filter by ASIA
    asia_nations = nations_df[nations_df['N_REGIONKEY'] == 'ASIA']['N_NATIONKEY'].unique()
    line_parts_suppliers_asia = line_parts_suppliers[line_parts_suppliers['S_NATIONKEY'].isin(asia_nations)]
    
    # Compute revenue and filter by the required years
    line_parts_suppliers_asia['REVENUE'] = line_parts_suppliers_asia['L_EXTENDEDPRICE'] * (1 - line_parts_suppliers_asia['L_DISCOUNT'])
    years_revenue = line_parts_suppliers_asia.groupby(line_parts_suppliers_asia['L_SHIPDATE'].str[:4])['REVENUE'].sum().to_dict()
    total_revenue_asia = sum(years_revenue.values())
    
    # Calculate market shares for 1995 and 1996
    market_shares = calculate_market_share(years_revenue, total_revenue_asia)
    
    # Write the result to a CSV file
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['1995 Market Share', '1996 Market Share'])
        writer.writerow(market_shares)

if __name__ == "__main__":
    main()
