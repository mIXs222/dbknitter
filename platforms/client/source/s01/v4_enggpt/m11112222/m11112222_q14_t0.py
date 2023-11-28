import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB Connection
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
part_collection = mongo_db['part']

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379)

# MongoDB Query for relevant parts
promo_parts = part_collection.find({
    'P_TYPE': {
        '$regex': '^PROMO'
    }
}, {'_id': 0, 'P_PARTKEY': 1})

# Extract part keys for promo parts
promo_part_keys = [part['P_PARTKEY'] for part in promo_parts]

# Redis Query for relevant lineitems
start_date = "1995-09-01"
end_date = "1995-09-30"
date_format = "%Y-%m-%d"
lineitems_df = pd.DataFrame(redis_client.get('lineitem'))

if lineitems_df.empty:
    print("No lineitem data found.")
else:
    # Filter by date range and by promo_part_keys
    lineitems_df['L_SHIPDATE'] = pd.to_datetime(lineitems_df['L_SHIPDATE'])
    date_mask = (lineitems_df['L_SHIPDATE'] >= datetime.strptime(start_date, date_format)) & \
                (lineitems_df['L_SHIPDATE'] <= datetime.strptime(end_date, date_format))
    promo_mask = lineitems_df['L_PARTKEY'].isin(promo_part_keys)
    filtered_lineitems_df = lineitems_df[date_mask & promo_mask]
    
    # Calculation for promotional revenue and total revenue
    filtered_lineitems_df['ADJ_EXTENDEDPRICE'] = filtered_lineitems_df['L_EXTENDEDPRICE'] * \
                                                 (1 - filtered_lineitems_df['L_DISCOUNT'])
    promo_revenue = filtered_lineitems_df['ADJ_EXTENDEDPRICE'].sum()
    total_revenue = lineitems_df[date_mask]['ADJ_EXTENDEDPRICE'].sum()

    # Calculating promotional revenue percentage
    promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

    # Output to file
    result = pd.DataFrame([{'Promotional Revenue': promo_revenue, 
                            'Total Revenue': total_revenue, 
                            'Promotional Revenue %': promo_revenue_percentage}])
    result.to_csv('query_output.csv', index=False)
    print("Query results have been saved to query_output.csv")
