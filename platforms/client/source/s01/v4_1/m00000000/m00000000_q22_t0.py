import pandas as pd
import pymysql.cursors
from pymongo import MongoClient

# Connect to the mysql database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
try:
    with connection.cursor() as cursor:
        sql = """
        SELECT
            SUBSTRING(C_PHONE, 1 , 2) AS CNTRYCODE,
            C_ACCTBAL
        FROM
            customer
        WHERE
            SUBSTRING(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > (
                SELECT
                    AVG(C_ACCTBAL)
                FROM
                    customer
                WHERE
                    C_ACCTBAL > 0.00
                AND SUBSTRING(C_PHONE , 1 , 2) IN
                ('20', '40', '22', '30', '39', '42', '21')
        )
        """
        cursor.execute(sql)
        result = cursor.fetchall()
        
        df1 = pd.DataFrame(result)

finally:
    connection.close()

# Connect to the MongoDB server
client = MongoClient()
db = client.sample

# Query is not applicable because there is no corresponding tables in MongoDB

# Save the query result to a CSV file
df1.to_csv('query_output.csv', index=False)
