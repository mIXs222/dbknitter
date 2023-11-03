import pymysql
import pymongo
import pandas as pd

mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

mongo_conn = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_conn["tpch"]

supplier = pd.DataFrame(list(mongo_db.supplier.find({})))
nation = pd.DataFrame(list(mongo_db.nation.find({})))

df1 = supplier.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df1 = df1[df1['N_NAME'] == 'GERMANY'][['S_SUPPKEY', 'S_NATIONKEY']]

query = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST, PS_AVAILQTY
FROM partsupp
"""
df2 = pd.read_sql(query, con=mysql_conn)

df = df1.merge(df2, on="S_SUPPKEY")

df['VALUE'] = df['PS_SUPPLYCOST'] * df['PS_AVAILQTY']
df = df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()

total_value = df['VALUE'].sum() * 0.0001000000
df = df[df['VALUE'] > total_value]

df.sort_values(by='VALUE', ascending=False, inplace=True)
df.to_csv('query_output.csv', index=False)
