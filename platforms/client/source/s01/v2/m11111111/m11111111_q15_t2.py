#python_code.py

from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

lineitem = db["lineitem"]
supplier = db["supplier"]

pipeline = [
    {"$match" : {"L_SHIPDATE" : {"$gte" : datetime(1996, 1, 1), "$lt" : datetime(1996, 1, 1) + relativedelta(months=+3)}}}, 
    {"$group" : {"_id" : "$L_SUPPKEY", "total_revenue" : {"$sum" : {"$multiply" : ["$L_EXTENDEDPRICE" , {"$subtract" : [1, "$L_DISCOUNT"]}]} }}}
]

revenue0 = pd.DataFrame(list(lineitem.aggregate(pipeline)))
revenue0.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']
revenue0 = revenue0.set_index('SUPPLIER_NO')

max_revenue = revenue0['TOTAL_REVENUE'].max()

suppliers = pd.DataFrame(list(supplier.find({}, {'_id':0, 'S_SUPPKEY':1, 'S_NAME':1, 'S_ADDRESS':1, 'S_PHONE':1})))
suppliers = suppliers.set_index('S_SUPPKEY')

joined = suppliers.join(revenue0, how='inner')
max_revenue_supp = joined[joined['TOTAL_REVENUE'] == max_revenue]

max_revenue_supp.to_csv('query_output.csv')
