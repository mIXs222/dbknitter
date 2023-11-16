import pymysql
import csv

mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')


def get_sql_data(sql):
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return rows


def write_data_to_csv(data, filename):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data)


query = """
        SELECT
            C_NAME,
            C_CUSTKEY,
            O_ORDERKEY,
            O_ORDERDATE,
            O_TOTALPRICE,
            SUM(L_QUANTITY)
        FROM
            customer,
            orders,
            lineitem
        WHERE
            O_ORDERKEY IN (
            SELECT
                L_ORDERKEY
            FROM
                lineitem
            GROUP BY
                L_ORDERKEY HAVING
                SUM(L_QUANTITY) > 300
            )
        AND C_CUSTKEY = O_CUSTKEY
        AND O_ORDERKEY = L_ORDERKEY
        GROUP BY
            C_NAME,
            C_CUSTKEY,
            O_ORDERKEY,
            O_ORDERDATE,
            O_TOTALPRICE
        ORDER BY
            O_TOTALPRICE DESC,
            O_ORDERDATE
        """

output_data = get_sql_data(query)

write_data_to_csv(output_data, 'query_output.csv')
