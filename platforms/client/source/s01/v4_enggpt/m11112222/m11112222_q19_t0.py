import pymongo
import direct_redis
import pandas as pd

# Connect to MongoDB to fetch parts data
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
parts_collection = mongo_db['part']
parts = pd.DataFrame(list(parts_collection.find()))

# Define Redis connection and fetch lineitem data
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem = r.get('lineitem')

# Criteria according to the query
criteria = [
    {
        'brand': 'Brand#12',
        'containers': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
        'quantity': (1, 11),
        'size': (1, 5),
        'modes': ['AIR', 'AIR REG']
    },
    {
        'brand': 'Brand#23',
        'containers': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
        'quantity': (10, 20),
        'size': (1, 10),
        'modes': ['AIR', 'AIR REG']
    },
    {
        'brand': 'Brand#34',
        'containers': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'],
        'quantity': (20, 30),
        'size': (1, 15),
        'modes': ['AIR', 'AIR REG']
    }
]

# Filter parts according to the criteria
filtered_parts = pd.DataFrame()
for c in criteria:
    filtered_parts = filtered_parts.append(parts[
        (parts['P_BRAND'] == c['brand']) &
        (parts['P_CONTAINER'].isin(c['containers'])) &
        (parts['P_SIZE'] >= c['size'][0]) &
        (parts['P_SIZE'] <= c['size'][1])
    ], ignore_index=True)

# Join lineitem with filtered parts
lineitem_filtered = lineitem.merge(filtered_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter on quantity, mode, and instruction
final_dataframe = pd.DataFrame()
for c in criteria:
    final_dataframe = final_dataframe.append(lineitem_filtered[
        (lineitem_filtered['L_QUANTITY'] >= c['quantity'][0]) &
        (lineitem_filtered['L_QUANTITY'] <= c['quantity'][1]) &
        (lineitem_filtered['L_SHIPMODE'].isin(c['modes'])) &
        (lineitem_filtered['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ], ignore_index=True)

# Calculate revenue
final_dataframe['revenue'] = final_dataframe['L_EXTENDEDPRICE'] * (1 - final_dataframe['L_DISCOUNT'])

# Write output to CSV
final_dataframe.to_csv('query_output.csv', index=False)
