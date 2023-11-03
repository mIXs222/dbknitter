import pandas as pd
from sqlalchemy import create_engine
import pymongo
import simplejson as json

engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")

# Execute SQL on MySQL using pandas
my_sql_query = """
SELECT AVG(C_ACCTBAL) as avg_acctbal FROM customer WHERE C_ACCTBAL > 0.00
"""

avg_acctbal = pd.read_sql(my_sql_query, engine)
avg_acctbal = avg_acctbal['avg_acctbal'][0]

my_client = pymongo.MongoClient("mongodb://mongodb:27017/")
my_db = my_client["tpch"]

# Select data from MongoDB
mongo_query = {"$and": [{"C_ACCTBAL": {"$gt": avg_acctbal}}, {"C_PHONE": {"$in": ['20', '40', '22', '30', '39', '42', '21']}}]}
projection = {"C_PHONE": 1, "C_ACCTBAL": 1, "_id": 0}
customers = my_db['customer'].find(mongo_query, projection)

# Convert data to pandas DataFrame
df = pd.DataFrame(list(customers))
df['CNTRYCODE'] = df['C_PHONE'].str[0:2]
df = df[df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]
df = df.groupby('CNTRYCODE').agg({'C_ACCTBAL': ['count', 'sum']}).reset_index()
df.columns = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']

# Write output to csv
df.to_csv('query_output.csv', index=False)
