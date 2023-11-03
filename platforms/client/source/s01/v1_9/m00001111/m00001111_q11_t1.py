from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# Create connection to MySQL
engine = create_engine('mysql+pymysql://root:my-secret-pw@mysql/tpch')

# Create connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query data from MySQL
query = """
    SELECT S_SUPPKEY, N_NATIONKEY FROM SUPPLIER
    INNER JOIN NATION ON S_NATIONKEY = N_NATIONKEY
    WHERE N_NAME = 'GERMANY'
"""
mysql_df = pd.read_sql_query(query, engine)

# Query data from MongoDB
pipeline = [
    {"$match": {"PS_SUPPKEY": {"$in": mysql_df['S_SUPPKEY'].tolist()}}},
    {"$group": {"_id": "$PS_PARTKEY", "VALUE": {"$sum": {"$multiply": ["$PS_SUPPLYCOST", "$PS_AVAILQTY"]}}}},
    {"$match": {"VALUE": {"$gt": mysql_df['VALUE'].sum() * 0.0001}}},
    {"$sort": {"VALUE": -1}}
]
mongo_df = pd.DataFrame(list(db['partsupp'].aggregate(pipeline)))

# Write the output to a CSV file
mongo_df.to_csv('query_output.csv', index=False)
