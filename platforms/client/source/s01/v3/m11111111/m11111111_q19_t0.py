from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

lineitems = list(db['lineitem'].find())
parts = list(db['part'].find())


lineitem_df = pd.DataFrame(lineitems)
parts_df = pd.DataFrame(parts) 

combined_df = pd.merge(lineitem_df, parts_df, how="inner", left_on="L_PARTKEY", right_on="P_PARTKEY")

conditions = [
    ((combined_df['P_BRAND'] == 'Brand#12') &
    combined_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) &
    combined_df['L_QUANTITY'].between(1, 1+10) & combined_df['P_SIZE'].between(1, 5) & 
    combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
    (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')),

    ((combined_df['P_BRAND'] == 'Brand#23') &
    combined_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']) &
    combined_df['L_QUANTITY'].between(10, 10+10) & combined_df['P_SIZE'].between(1, 10) & 
    combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
    (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')),

    ((combined_df['P_BRAND'] == 'Brand#34') &
    combined_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']) &
    combined_df['L_QUANTITY'].between(20, 20+10) & combined_df['P_SIZE'].between(1, 15) & 
    combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) & 
    (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

combined_df['REVENUE'] = combined_df.loc[conditions, 'L_EXTENDEDPRICE'] * (1 - combined_df.loc[conditions,'L_DISCOUNT'])

combined_df.to_csv("query_output.csv")
