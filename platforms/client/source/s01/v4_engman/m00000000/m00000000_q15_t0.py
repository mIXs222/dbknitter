import csv
import pymysql

# Function to connect to MySQL and execute query
def run_mysql_query():
    # MySQL connection parameters
    mysql_conn_params = {
        "database": "tpch",
        "user": "root",
        "password": "my-secret-pw",
        "host": "mysql"
    }

    # SQL Query
    sql = """
    SELECT 
        s.S_SUPPKEY, 
        s.S_NAME, 
        s.S_ADDRESS, 
        s.S_PHONE, 
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS TOTAL_REVENUE
    FROM 
        supplier AS s
    JOIN 
        lineitem AS l ON s.S_SUPPKEY = l.L_SUPPKEY
    WHERE 
        l.L_SHIPDATE BETWEEN '1996-01-01' AND '1996-04-01'
    GROUP BY 
        s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE
    ORDER BY 
        TOTAL_REVENUE DESC, s.S_SUPPKEY
    """

    # Connect to MySQL
    connection = pymysql.connect(**mysql_conn_params)
    cursor = connection.cursor()
    cursor.execute(sql)

    # Fetch the data
    data = cursor.fetchall()
    max_revenue = data[0][4] if data else None
    filtered_data = [row for row in data if row[4] == max_revenue]

    # Close the database connection
    cursor.close()
    connection.close()

    return filtered_data


# Write the output to a CSV file
def write_to_csv(file_name, data):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"])
        writer.writerows(data)


def main():
    # Run the query on MySQL
    mysql_data = run_mysql_query()

    # Output results to query_output.csv
    write_to_csv("query_output.csv", mysql_data)

if __name__ == "__main__":
    main()
