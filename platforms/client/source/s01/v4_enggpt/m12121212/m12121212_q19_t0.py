import pymongo
import redis
import pandas as pd

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part = pd.DataFrame(list(mongo_db.part.find(
    {"$or": [
        {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
        {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
        {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}}
    ]}
)))

# Redis connection and query
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
lineitem = pd.read_json(r.get('lineitem'), orient='records')

# Filtering the lineitem DataFrame based on the query
filtered_lineitem = lineitem[
    (
        # For parts with 'Brand#12'
        (lineitem['L_SHIPMODE'].isin(["AIR", "AIR REG"]) &
        (lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') & 
        (lineitem['L_QUANTITY'] >= 1) & (lineitem['L_QUANTITY'] <= 11)) |
        
        # For parts with 'Brand#23'
        (lineitem['L_SHIPMODE'].isin(["AIR", "AIR REG"]) & 
        (lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') & 
        (lineitem['L_QUANTITY'] >= 10) & (lineitem['L_QUANTITY'] <= 20)) |
        
        # For parts with 'Brand#34'
        (lineitem['L_SHIPMODE'].isin(["AIR", "AIR REG"]) & 
        (lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') & 
        (lineitem['L_QUANTITY'] >= 20) & (lineitem['L_QUANTITY'] <= 30))
    )
]

# Joining parts and lineitems on P_PARTKEY == L_PARTKEY
joined_data = pd.merge(filtered_lineitem, part, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filtering the joined data based on the size for each brand
joined_data_filtered = joined_data[
    ((joined_data['P_BRAND'] == 'Brand#12') & (joined_data['P_SIZE'] >= 1) & (joined_data['P_SIZE'] <= 5)) |
    ((joined_data['P_BRAND'] == 'Brand#23') & (joined_data['P_SIZE'] >= 1) & (joined_data['P_SIZE'] <= 10)) |
    ((joined_data['P_BRAND'] == 'Brand#34') & (joined_data['P_SIZE'] >= 1) & (joined_data['P_SIZE'] <= 15))
]

# Calculating revenue
joined_data_filtered['REVENUE'] = joined_data_filtered['L_EXTENDEDPRICE'] * (1 - joined_data_filtered['L_DISCOUNT'])

# Summing up revenue
total_revenue = joined_data_filtered['REVENUE'].sum()

# Storing the result to a csv file
output = pd.DataFrame([{'Total Revenue': total_revenue}])
output.to_csv('query_output.csv', index=False)
