from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# MySQL connection
engine = create_engine('mysql+pymysql://root:my-secret-pw@mysql:3306/tpch')
# MongoDB connection
client = MongoClient('mongodb:27017')
db = client.tpch


# Query for MySQL
query1 = '''
SELECT
    PS_PARTKEY,
    PS_SUPPLYCOST,
    PS_AVAILQTY
FROM
    partsupp
'''
# Execute the query and convert to DataFrame
df1 = pd.read_sql_query(query1, engine)

# Query for MongoDB
query2 = '''
SELECT
    S_SUPPKEY,
    S_NATIONKEY
FROM
    supplier
'''
supplier_collection = db.supplier
df2 = pd.DataFrame(list(supplier_collection.find()))

# Merge MySQL and MongoDB data
merged_df = df1.merge(df2,left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by PS_PARTKEY
group_df = merged_df.groupby('PS_PARTKEY').sum()

# Filter the data
final_df = group_df[group_df['PS_SUPPLYCOST'] * group_df['PS_AVAILQTY'] > group_df['PS_SUPPLYCOST'].sum() * 0.0001]

# write the output to a file
final_df.to_csv('query_output.csv')
