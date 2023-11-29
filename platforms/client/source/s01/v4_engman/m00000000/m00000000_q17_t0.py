import pymysql
import csv

# Connection details
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_connection_info)
try:
    with mysql_conn.cursor() as cursor:
        # Calculate the average quantity of parts for the brand and container type
        avg_quantity_sql = """
        SELECT AVG(L_QUANTITY)
        FROM lineitem AS l
        JOIN part AS p ON l.L_PARTKEY = p.P_PARTKEY
        WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG'
        """
        cursor.execute(avg_quantity_sql)
        avg_quantity = cursor.fetchone()[0] if cursor.rowcount > 0 else 0

        # Calculate the yearly revenue loss assuming there's one less eligible order per year
        if avg_quantity > 0:
            loss_sql = """
            SELECT
                AVG((L_QUANTITY / %s) * L_EXTENDEDPRICE) AS avg_yearly_loss
            FROM lineitem AS l
            JOIN part AS p ON l.L_PARTKEY = p.P_PARTKEY
            WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG' AND L_QUANTITY < %s * 0.2
            """
            cursor.execute(loss_sql, (avg_quantity, avg_quantity))
            avg_yearly_loss = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
        else:
            avg_yearly_loss = 0

    # Output result to a CSV file
    with open('query_output.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['avg_yearly_loss'])
        writer.writerow([avg_yearly_loss])

finally:
    mysql_conn.close()
