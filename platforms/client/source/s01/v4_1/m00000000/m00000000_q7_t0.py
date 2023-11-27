import pandas as pd
import pymysql
import datetime

def get_data_from_mysql():
    # Connect to the MySQL Server
    db = pymysql.connect(host="mysql", user="root", password="my-secret-pw", db="tpch")
    cursor = db.cursor()

    # Write the SQL query
    query = """
        SELECT
            n1.N_NAME AS SUPP_NATION,
            n2.N_NAME AS CUST_NATION,
            YEAR(l.L_SHIPDATE) AS L_YEAR,
            l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS VOLUME
        FROM
            supplier s,
            lineitem l,
            orders o,
            customer c,
            nation n1,
            nation n2
        WHERE
            s.S_SUPPKEY = l.L_SUPPKEY
            AND o.O_ORDERKEY = l.L_ORDERKEY
            AND c.C_CUSTKEY = o.O_CUSTKEY
            AND s.S_NATIONKEY = n1.N_NATIONKEY
            AND c.C_NATIONKEY = n2.N_NATIONKEY
            AND (
                (n1.N_NAME = 'JAPAN' AND n2.N_NAME = 'INDIA')
                OR (n1.N_NAME = 'INDIA' AND n2.N_NAME = 'JAPAN')
                )
            AND l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    # Execute the query
    cursor.execute(query)

    # Fetch the results
    result = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    db.close()

    return result

def write_to_csv(data, filename):
    df = pd.DataFrame(data, columns=["SUPP_NATION", "CUST_NATION", "L_YEAR", "VOLUME"])
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    # Run the query
    data = get_data_from_mysql()

    # Write the results to a csv file
    write_to_csv(data, "query_output.csv")
