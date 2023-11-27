import mysql.connector
from pymongo import MongoClient
import pandas as pd

# connect to mysql
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# execute mysql query
mysql_query = """
SELECT
    N_NAME,
    O_ORDERKEY,
    O_CUSTKEY,
    O_TOTALPRICE,
    O_ORDERDATE
FROM
    nation,
    orders
WHERE
    O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
"""
mysql_df = pd.read_sql(sql=mysql_query, con=mysql_conn)

# connect to mongodb
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# execute mongodb query
mongo_customer_df = pd.DataFrame(mongo_db.customer.find({}, 
                                        {'C_CUSTKEY': 1, 'C_NAME': 1, 'C_ACCTBAL': 1, 'C_PHONE': 1, 'C_NATIONKEY': 1, 'C_ADDRESS': 1, 'C_COMMENT': 1}))
mongo_lineitem_df = pd.DataFrame(mongo_db.lineitem.find({}, 
                                        {'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_RETURNFLAG': 1}))

# merge all dataframes
merged_df = pd.merge(mysql_df, mongo_customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = pd.merge(merged_df, mongo_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# group by necessary columns
grouped_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT', 'REVENUE'])

# write to csv file
grouped_df.to_csv('query_output.csv')
