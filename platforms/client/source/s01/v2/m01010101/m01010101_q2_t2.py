from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# Connect to mysql
engine = create_engine('mysql+pymysql://root:my-secret-pw@mysql:3306/tpch')
con = engine.connect()

# Fetch data from mysql
q1 = """
SELECT N_NATIONKEY, N_NAME FROM nation
"""
nation = pd.read_sql_query(q1, con)

q2 = """
SELECT P_PARTKEY, P_MFGR, P_SIZE, P_TYPE FROM part
"""
part = pd.read_sql_query(q2, con)

q3 = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST FROM partsupp
"""
partsupp = pd.read_sql_query(q3, con)

# Connect to mongodb
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch data from mongodb
region = pd.DataFrame(list(db.region.find({}, {'_id': 0, 'R_REGIONKEY': 1, 'R_NAME': 1})))
supplier = pd.DataFrame(list(db.supplier.find({},{'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_NATIONKEY': 1, 'S_PHONE': 1, 'S_COMMENT': 1, 'S_ACCTBAL': 1})))

# Merge and filter data
merged = pd.merge(pd.merge(pd.merge(pd.merge(part, partsupp, on='PS_PARTKEY'), nation, on='N_NATIONKEY'), supplier, on='S_SUPPKEY'), region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
filt = merged[(merged['P_SIZE']==15) & (merged['P_TYPE'].str.contains('BRASS')) & (merged['R_NAME']=='EUROPE') & (merged['PS_SUPPLYCOST'] == merged['PS_SUPPLYCOST'].min())]

# Sort and write to csv
filt = filt.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
filt.to_csv('query_output.csv', index=False)

con.close()
client.close()
