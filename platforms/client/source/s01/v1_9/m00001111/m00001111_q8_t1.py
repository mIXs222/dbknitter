from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# Establishing connection with MySQL
engine = create_engine(f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}")

# Read MySQL tables into dataframes
part_df = pd.read_sql("SELECT * FROM part", engine)
supplier_df = pd.read_sql("SELECT * FROM supplier", engine)
nation_df = pd.read_sql("SELECT * FROM nation", engine)
region_df = pd.read_sql("SELECT * FROM region", engine)

# Establishing MongoDB client
client = MongoClient(f"mongodb://{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}")

# Assigning the database
db = client[os.getenv('MONGODB_DATABASE')]

# Loading MongoDB collections into dataframes
lineitem_df = pd.DataFrame(list(db['lineitem'].find()))
orders_df = pd.DataFrame(list(db['orders'].find()))
customer_df = pd.DataFrame(list(db['customer'].find()))
partsupp_df = pd.DataFrame(list(db['partsupp'].find()))

#Concatenating all dataframes into single dataframe
all_dfs = [part_df, supplier_df, nation_df, region_df, lineitem_df, orders_df, customer_df, partsupp_df]

final_df = pd.concat(all_dfs, axis=0, ignore_index=True)

# Writing the query
query = '''
SELECT
    O_YEAR,
    SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
FROM
    (
    SELECT
        strftime('%Y', O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
        N2.N_NAME AS NATION
    FROM
        final_df
    WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL PLATED COPPER'
    ) AS ALL_NATIONS
GROUP BY
    O_YEAR
    ORDER BY
    O_YEAR
'''

# Executing the query and saving the outcome to a dataframe
out_df = pd.read_sql_query(query, final_df)

# Writing the dataframe to a .csv file
out_df.to_csv('query_output.csv', index=False)
