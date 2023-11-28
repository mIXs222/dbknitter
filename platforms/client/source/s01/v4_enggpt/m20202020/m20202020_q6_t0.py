import csv
import pymysql
import datetime

# Function to connect to the MySQL database
def connect_to_mysql():
    return pymysql.connect(
        host='mysql', 
        user='root', 
        password='my-secret-pw', 
        db='tpch',
        charset='utf8mb4'
    )
    
# Function to fetch data from the MySQL database
def fetch_data(connection):
    with connection.cursor() as cursor:
        query = """
            SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as total_revenue
            FROM lineitem
            WHERE
                L_SHIPDATE >= %s AND L_SHIPDATE <= %s AND
                L_DISCOUNT BETWEEN %s AND %s AND
                L_QUANTITY < %s
        """
        # Define the parameters for the query
        params = (
            datetime.date(1994, 1, 1),
            datetime.date(1994, 12, 31),
            0.05,  # 5% discount
            0.07,  # 7% discount
            24     # quantity less than
        )
        
        # Execute the query using the parameters
        cursor.execute(query, params)
        result = cursor.fetchone()
        return result[0] if result else None

def main():
    connection = connect_to_mysql()
    try:
        total_revenue = fetch_data(connection)
        
        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['total_revenue']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            if total_revenue is not None:
                writer.writerow({'total_revenue': total_revenue})
                
    finally:
        connection.close()

if __name__ == '__main__':
    main()
