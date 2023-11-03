import pymysql.cursors
import pandas as pd


# Function to connect to the MySQL database
def create_conn():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

# Function to execute the SQL query and write the output to a CSV file
def execute_query():
    conn = create_conn()
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT
                SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
            FROM
                LINEITEM,
                PART
            WHERE
                P_PARTKEY = L_PARTKEY
                AND P_BRAND = 'Brand#23'
                AND P_CONTAINER = 'MED BAG'
                AND L_QUANTITY < (
                    SELECT
                        0.2 * AVG(L_QUANTITY)
                    FROM
                        LINEITEM
                    WHERE
                        L_PARTKEY = P_PARTKEY
                )
            """
            cursor.execute(query)
            result = cursor.fetchall()

            # convert the result to pandas dataframe
            df = pd.DataFrame(result)

            # write the dataframe to a CSV file
            df.to_csv('query_output.csv', index=False)

    finally:
        conn.close()


if __name__ == "__main__":
    execute_query()
