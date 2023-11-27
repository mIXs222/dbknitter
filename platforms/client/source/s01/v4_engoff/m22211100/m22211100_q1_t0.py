import csv
import pymysql

# MySQL connection
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    cursor = connection.cursor()
    try:
        query = """
        SELECT
            L_RETURNFLAG,
            L_LINESTATUS,
            SUM(L_QUANTITY) as sum_qty,
            SUM(L_EXTENDEDPRICE) as sum_base_price,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as sum_disc_price,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as sum_charge,
            AVG(L_QUANTITY) as avg_qty,
            AVG(L_EXTENDEDPRICE) as avg_price,
            AVG(L_DISCOUNT) as avg_disc,
            COUNT(*) as count_order
        FROM
            lineitem
        WHERE
            L_SHIPDATE <= '1998-09-01'
        GROUP BY
            L_RETURNFLAG,
            L_LINESTATUS
        ORDER BY
            L_RETURNFLAG,
            L_LINESTATUS;
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()
        connection.close()

# Write to CSV file
def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE',
                         'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
        for row in data:
            writer.writerow(row)

if __name__ == "__main__":
    mysql_data = get_mysql_data()
    write_to_csv(mysql_data, 'query_output.csv')
