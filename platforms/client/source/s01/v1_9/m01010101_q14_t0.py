import os
import pandas as pd
from pymongo import MongoClient
import mysql.connector

def get_mongo_data():
    mongo_client = MongoClient('mongodb', 27017)
    db = mongo_client['tpch']
    lineitem_data = pd.DataFrame(list(db.lineitem.find({}, {"_id": 0})))
    part_data = pd.DataFrame(list(db.part.find({}, {"_id": 0})))
    return lineitem_data, part_data

def get_mysql_data():
    db_connection = mysql.connector.connect(user='root', password='my-secret-pw', 
                                            host='mysql', database='tpch')
    cursor = db_connection.cursor()

    query = ("SELECT * FROM lineitem")
    cursor.execute(query)
    lineitem_data = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

    query = ("SELECT * FROM part")
    cursor.execute(query)
    part_data = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

    cursor.close()
    db_connection.close()

    return lineitem_data, part_data

def compute(query_output):
    lineitem_data, part_data = get_mongo_data()
    merged_data = pd.merge(lineitem_data, part_data, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    merged_data['L_SHIPDATE'] = pd.to_datetime(merged_data['L_SHIPDATE'])

    mask = (merged_data['L_SHIPDATE'] >= '1995-09-01') & (merged_data['L_SHIPDATE'] < '1995-10-01')
    required_data = merged_data.loc[mask]
    required_data['SALES'] = required_data['L_EXTENDEDPRICE'] * (1 - required_data['L_DISCOUNT'])
    required_data['PROMO_SALES'] = required_data.apply(lambda row: row['SALES'] if row['P_TYPE'].startswith('PROMO') else 0, axis=1)
    total_sales = required_data['SALES'].sum()
    promo_sales = required_data['PROMO_SALES'].sum()
    promo_revenue = (100.00 * promo_sales) / total_sales
    query_output_dict = {'PROMO_REVENUE': [promo_revenue]}

    query_output_df = pd.DataFrame.from_dict(query_output_dict)
    query_output_df.to_csv(query_output, index=False)

if __name__ == "__main__":
    compute('query_output.csv')
