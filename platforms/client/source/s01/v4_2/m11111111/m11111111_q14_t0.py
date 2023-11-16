from datetime import datetime
from pymongo import MongoClient
import pandas as pd

def calculate_promo_revenue(part_df, lineitem_df):

    merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    merged_df['L_SHIPDATE'] = pd.to_datetime(merged_df['L_SHIPDATE'])
    condition_shipdate = ((merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01'))

    merged_df['DISCOUNT_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
    merged_df.loc[merged_df['P_TYPE'].str.startswith("PROMO"), 'PROMO_DISCOUNT'] = merged_df['DISCOUNT_PRICE']
    merged_df['PROMO_DISCOUNT'].fillna(0, inplace=True)

    promo_revenue = (merged_df['PROMO_DISCOUNT'].sum() / merged_df['DISCOUNT_PRICE'].sum()) * 100.00

    result_df = pd.DataFrame(columns=['PROMO_REVENUE'])
    result_df = result_df.append({'PROMO_REVENUE' : promo_revenue}, ignore_index=True)
    result_df.to_csv("query_output.csv", index=False)

client = MongoClient("mongodb", 27017)
db = client['tpch']

part_data = list(db['part'].find())
lineitem_data = list(db['lineitem'].find())

part_df = pd.DataFrame(part_data)
lineitem_df = pd.DataFrame(lineitem_data)

calculate_promo_revenue(part_df, lineitem_df)

