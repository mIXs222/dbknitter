import pymysql
import csv

mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

def connect_to_mysql():
    try:
        return pymysql.connect(
            host=mysql_connection_info['host'],
            user=mysql_connection_info['user'],
            password=mysql_connection_info['password'],
            db=mysql_connection_info['db'],
        )
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return None

def execute_query(connection):
    query = """
        SELECT COUNT(DISTINCT o.O_ORDERKEY) AS ORDER_COUNT, 
               o.O_ORDERPRIORITY 
        FROM orders o 
        JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY 
        WHERE o.O_ORDERDATE >= '1993-07-01' AND 
              o.O_ORDERDATE <  '1993-10-01' AND 
              l.L_COMMITDATE < l.L_RECEIPTDATE 
        GROUP BY o.O_ORDERPRIORITY 
        ORDER BY o.O_ORDERPRIORITY ASC;
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"Error executing query on MySQL Platform: {e}")

def save_results_to_csv(results, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['ORDER_COUNT', 'O_ORDERPRIORITY'])
        csvwriter.writerows(results)

def main():
    mysql_connection = connect_to_mysql()
    if mysql_connection:
        try:
            results = execute_query(mysql_connection)
            if results:
                save_results_to_csv(results, 'query_output.csv')
        finally:
            mysql_connection.close()

if __name__ == "__main__":
    main()
