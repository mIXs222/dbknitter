from pymongo import MongoClient
import mysql.connector
from decimal import Decimal
import csv

def get_query_results():
    mongodb_client = MongoClient('mongodb', 27017)
    mongodb_db = mongodb_client['tpch']
    lineitem_table = mongodb_db['lineitem']

    lineitem_data = lineitem_table.find({
        'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
        'L_DISCOUNT': {'$gte': Decimal('.06') - 0.01, '$lte': Decimal('.06') + 0.01},
        'L_QUANTITY': {'$lt': 24}
    })

    mysql_db = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )
    mysql_cursor = mysql_db.cursor()

    revenue = 0
    for row in lineitem_data:
        mysql_cursor.execute(f"SELECT L_EXTENDEDPRICE, L_DISCOUNT FROM lineitem WHERE L_ORDERKEY = {row['L_ORDERKEY']}")
        L_EXTENDEDPRICE, L_DISCOUNT = mysql_cursor.fetchone()
        revenue += (L_EXTENDEDPRICE * L_DISCOUNT)

    return revenue

def write_results_to_csv(revenue):
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["REVENUE"])
        writer.writerow([str(revenue)])

revenue = get_query_results()
write_results_to_csv(revenue)
