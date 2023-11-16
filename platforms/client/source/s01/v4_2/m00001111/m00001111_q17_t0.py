import csv
import pymysql
from pymongo import MongoClient
import pandas as pd

def calculate_average():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')

    cursor = connection.cursor()

    query1 = """
    SELECT
        P_PARTKEY,
        P_BRAND,
        P_CONTAINER
    FROM
        part
    WHERE
        P_BRAND = 'Brand#23'
        AND P_CONTAINER = 'MED BAG'
    """

    cursor.execute(query1)
    result1 = cursor.fetchall()

    client = MongoClient("mongodb", 27017)
    db = client.tpch
    lineitem = db.lineitem

    df1 = pd.DataFrame(list(result1), columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER'])

    avg_yearly = 0.0

    for index, row in df1.iterrows():
        p_partkey = row['P_PARTKEY']

        l_quantity = lineitem.aggregate([{"$match": {"L_PARTKEY": p_partkey }},
                                         {"$group": {"_id": "$L_PARTKEY", "avgQuantity": {"$avg": "$L_QUANTITY"}}}])

        l_quantity = list(l_quantity)[0]['avgQuantity']

        l_extendedprice = lineitem.aggregate([{"$match": {"L_PARTKEY": p_partkey, "L_QUANTITY": {"$lt": 0.2*l_quantity}}},
                                              {"$group": {"_id": "$L_PARTKEY", "sumPrice": {"$sum": "$L_EXTENDEDPRICE"}}}])
        if list(l_extendedprice):
            avg_yearly += list(l_extendedprice)[0]['sumPrice'] / 7.0

    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['AVG_YEARLY'])
        writer.writerow([avg_yearly])

calculate_average()
