# query.py file

import pymysql
import pymongo
import csv
from datetime import datetime

def connect_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

def connect_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def get_mysql_data(connection):
    query = """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost,
            c.C_ACCTBAL,
            c.C_ADDRESS,
            c.C_PHONE,
            c.C_COMMENT,
            l.L_RETURNFLAG
        FROM
            customer c
        JOIN
            lineitem l ON c.C_CUSTKEY = l.L_SUPPKEY
        WHERE
            l.L_RETURNFLAG = 'R' AND
            l.L_SHIPDATE BETWEEN '1993-10-01' AND '1994-01-01'
        GROUP BY
            c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    return results

def get_mongodb_data(client):
    collection = client['nation']
    results = list(collection.find({}, {'_id': 0}))
    nation_dict = {item['N_NATIONKEY']: item['N_NAME'] for item in results}
    return nation_dict

def convert_mysql_to_document(mysql_result, nation_dict):
    return [
        {
            'customer_key': row[0],
            'customer_name': row[1],
            'revenue_lost': row[2],
            'account_balance': row[3],
            'address': row[4],
            'phone': row[5],
            'comment': row[6],
            'nation': nation_dict.get(row[0])
        }
        for row in mysql_result
    ]

def write_csv(data, file_name):
    keys = data[0].keys()
    with open(file_name, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

# Main execution
def main():
    mysql_conn = connect_mysql()
    mongodb_client = connect_mongodb()

    mysql_data = get_mysql_data(mysql_conn)
    nation_dict = get_mongodb_data(mongodb_client)
    documents = convert_mysql_to_document(mysql_data, nation_dict)

    # Sorting according to the specified order
    documents.sort(
        key=lambda x: (x['revenue_lost'], x['customer_key'], x['customer_name'], -x['account_balance'])
    )

    write_csv(documents, 'query_output.csv')
    mysql_conn.close()

if __name__ == "__main__":
    main()
