# Python code
import csv
import os.path
from pymongo import MongoClient
from mysql.connector import connect, Error

def mongo_query():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    lineitem = db['lineitem']
    result = lineitem.find({'L_SHIPDATE': {'$lte': '1998-09-02'}})
    return list(result)

def mysql_query(query, records_list):
    try:
        with connect(
            host="mysql",
            user="root",
            password="my-secret-pw",
            database="tpch"
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, records_list)
                result = cursor.fetchall()
                return result
    except Error as e:
        print(e)

def write_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

def main():
    data = mongo_query()
    query = """
    SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY) AS SUM_QTY,
        SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
        AVG(L_QUANTITY) AS AVG_QTY,
        AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
        AVG(L_DISCOUNT) AS AVG_DISC,
        COUNT(*) AS COUNT_ORDER
    FROM
        %s
    WHERE
        L_SHIPDATE <= '1998-09-02'
    GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
    ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS
    """
    records_list_template = ', '.join(['%s'] * len(data))
    mysql_data = mysql_query(query % records_list_template, tuple([record['L_ORDERKEY'] for record in data]))
    output_data = [dict(zip(mysql_data[0], record)) for record in mysql_data[1:]]
    write_to_csv(output_data, 'query_output.csv')

if __name__ == "__main__":
    main()
