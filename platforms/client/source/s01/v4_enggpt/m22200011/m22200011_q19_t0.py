import pymongo
import direct_redis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]
lineitem_df = pd.DataFrame(list(db["lineitem"].find()))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_json(r.get("part"))

# Filter the part dataframe
brand_conditions = {
    'Brand#12': {
        'size': range(1, 6),
        'container': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
        'quantity': range(1, 12)
    },
    'Brand#23': {
        'size': range(1, 11),
        'container': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
        'quantity': range(10, 21)
    },
    'Brand#34': {
        'size': range(1, 16),
        'container': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'],
        'quantity': range(20, 31)
    }
}

filtered_parts = pd.DataFrame()
for brand, condition in brand_conditions.items():
    filtered_parts = filtered_parts.append(
        part_df[(part_df['P_BRAND'] == brand) &
                (part_df['P_CONTAINER'].isin(condition['container'])) &
                (part_df['P_SIZE'].between(min(condition['size']), max(condition['size'])))],
        ignore_index=True
    )
filtered_part_keys = filtered_parts['P_PARTKEY'].unique()

# Filter the lineitem dataframe
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') &
    (lineitem_df['L_QUANTITY'].isin(range(1, 31))) &
    (lineitem_df['L_PARTKEY'].isin(filtered_part_keys))
]

# Calculate the revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
total_revenue = filtered_lineitems['REVENUE'].sum()

# Output the result to a CSV file
output_df = pd.DataFrame({"Total Revenue": [total_revenue]})
output_df.to_csv("query_output.csv", index=False)
