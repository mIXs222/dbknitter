from pymongo import MongoClient
import pandas as pd
from datetime import datetime

client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

lineitem = pd.DataFrame(list(db.lineitem.find()))
part = pd.DataFrame(list(db.part.find()))

merged_data = pd.merge(lineitem, part, left_on='L_PARTKEY', right_on='P_PARTKEY')

startdate = datetime.strptime('1995-09-01', '%Y-%m-%d')
enddate = datetime.strptime('1995-10-01', '%Y-%m-%d')

filtered_data = merged_data[(merged_data['L_SHIPDATE']>= startdate) & 
                            (merged_data['L_SHIPDATE']< enddate)]

filtered_data.loc[filtered_data.P_TYPE.str.contains('PROMO%'), 'PROMO_REVENUE'] = \
filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])
filtered_data['PROMO_REVENUE'].fillna(0, inplace=True)

promo_revenue = 100.00 * filtered_data['PROMO_REVENUE'].sum() / (filtered_data['L_EXTENDEDPRICE'] 
                                                                  * (1 - filtered_data['L_DISCOUNT'])).sum()

print(promo_revenue)
