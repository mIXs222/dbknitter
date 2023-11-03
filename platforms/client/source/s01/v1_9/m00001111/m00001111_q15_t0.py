import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pymongo import MongoClient

#initiate the engines
db_string = "mysql://root:my-secret-pw@mysql:3306/tpch"
mysql_engine = create_engine(db_string)

mongo_conn = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_conn["tpch"]

#MongoDB query to pandas DataFrame
lineitem = pd.DataFrame(list(mongo_db.lineitem.find({"L_SHIPDATE": {"$gte": np.datetime64('1996-01-01'), "$lt": np.datetime64('1996-04-01')}})))

#calculations from subqueries
lineitem['REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
lineitem = lineitem.groupby('L_SUPPKEY').sum()
lineitem = lineitem[lineitem['REVENUE'] == lineitem['REVENUE'].max()]

#MySQL query: get the dataframes
supplier_df = pd.read_sql('SELECT * FROM supplier', mysql_engine)

# Join the supplier_df with lineitem_data on 'S_SUPPKEY' and 'SUPPLIER_NO'
result = pd.merge(supplier_df, lineitem, left_on='S_SUPPKEY' , right_on='L_SUPPKEY', how='inner')

# Write the result to csv
result.to_csv('query_output.csv', index=False)
