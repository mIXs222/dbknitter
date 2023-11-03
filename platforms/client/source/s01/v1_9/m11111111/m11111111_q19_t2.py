from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']
lineitem = db.lineitem.find({})
part = db.part.find({})

df_lineitem = pd.DataFrame(list(lineitem))
df_part = pd.DataFrame(list(part))

df = pd.merge(df_lineitem, df_part, 
         left_on='L_PARTKEY', 
         right_on='P_PARTKEY',
         how='inner')

conditions = [
    ((df['P_BRAND'] == 'Brand#12') & 
     (df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
     (df['L_QUANTITY'].between(1, 11)) &
     (df['P_SIZE'].between(1, 5)) &
     (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')),

    ((df['P_BRAND'] == 'Brand#23') & 
     (df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
     (df['L_QUANTITY'].between(10, 20)) &
     (df['P_SIZE'].between(1, 10)) &
     (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')),

    ((df['P_BRAND'] == 'Brand#34') & 
     (df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
     (df['L_QUANTITY'].between(20, 30)) &
     (df['P_SIZE'].between(1, 15)) &
     (df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

df.loc[conditions, 'REVENUE'] = df['L_EXTENDEDPRICE']* (1 - df['L_DISCOUNT'])
df.loc[~df['REVENUE'].isnull()].to_csv('query_output.csv', index=False, columns=['REVENUE'])
