from pymongo import MongoClient
import pandas as pd
import csv

def run_query():
  tpch_db = MongoClient('mongodb', 27017)['tpch']

  suppliers_tbl = tpch_db['supplier'].aggregate([{}])
  lineitem_tbl = tpch_db['lineitem'].aggregate([{}])
  orders_tbl = tpch_db['orders'].aggregate([{}])
  nation_tbl = tpch_db['nation'].aggregate([{}])

  df_suppliers = pd.DataFrame(list(suppliers_tbl))
  df_lineitem = pd.DataFrame(list(lineitem_tbl))
  df_orders = pd.DataFrame(list(orders_tbl))
  df_nation = pd.DataFrame(list(nation_tbl))

  filtered_lineitem = df_lineitem[df_lineitem['L_RECEIPTDATE'] > df_lineitem['L_COMMITDATE']]
  merged_df = pd.merge(df_suppliers, filtered_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
  merged_df = pd.merge(merged_df, df_orders[df_orders['O_ORDERSTATUS'] == 'F'], left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
  merged_df = pd.merge(merged_df, df_nation[df_nation['N_NAME'] == 'SAUDI ARABIA'], left_on='N_NATIONKEY', right_on='S_NATIONKEY', how='inner')

  grouped_df = merged_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')
  grouped_df = grouped_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
  
  # Write to CSV
  grouped_df.to_csv('query_output.csv', index=False)

# execute the function
run_query()
