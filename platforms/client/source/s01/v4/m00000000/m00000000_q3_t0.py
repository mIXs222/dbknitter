import csv
import pymysql

def execute_query_and_write_to_csv():
    connection = pymysql.connect(host='mysql',
                                user='root',
                                password='my-secret-pw',
                                database='tpch',
                                cursorclass=pymysql.cursors.Cursor)
    
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    L_ORDERKEY,
                    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
                    O_ORDERDATE,
                    O_SHIPPRIORITY
                FROM
                    customer, orders, lineitem
                WHERE
                    C_MKTSEGMENT = 'BUILDING'
                    AND C_CUSTKEY = O_CUSTKEY
                    AND L_ORDERKEY = O_ORDERKEY
                    AND O_ORDERDATE < '1995-03-15'
                    AND L_SHIPDATE > '1995-03-15'
                GROUP BY
                    L_ORDERKEY,
                    O_ORDERDATE,
                    O_SHIPPRIORITY
                ORDER BY
                    REVENUE DESC,
                    O_ORDERDATE
            """
            cursor.execute(query)
            result = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            
            with open('query_output.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for row in result:
                    writer.writerow(row)
    finally:
        connection.close()

if __name__ == "__main__":
    execute_query_and_write_to_csv()
