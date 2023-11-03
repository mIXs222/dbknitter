import pymongo
import mysql.connector
from pandas import DataFrame

def main():
    mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
    mysql_cursor = mysql_conn.cursor()

    mongo_conn = pymongo.MongoClient('mongodb', 27017)
    mongo_db = mongo_conn['tpch']

    lineitem_collection = mongo_db['lineitem']
    partsupp_collection = mongo_db['partsupp']

    query = "SELECT P_PARTKEY, P_BRAND, P_CONTAINER FROM PART WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
    mysql_cursor.execute(query)
    parts_result = mysql_cursor.fetchall()

    avg_yearly = 0
    for record in parts_result:
        partkey, _, _ = record
        lineitems = lineitem_collection.find({'L_PARTKEY': partkey})
        quantities = [i['L_QUANTITY'] for i in lineitems]
        avg_quantity = sum(quantities) / len(quantities) if quantities else 0
        lineitems = lineitem_collection.find({'L_PARTKEY': partkey , 'L_QUANTITY': {'$lt': 0.2 * avg_quantity}})
        extended_prices = [i['L_EXTENDEDPRICE'] for i in lineitems]
        avg_yearly += sum(extended_prices)

    avg_yearly /= 7.0
    
    df = DataFrame([avg_yearly], columns=["AVG_YEARLY"])
    df.to_csv('query_output.csv', index=False)

    mysql_cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    main()
