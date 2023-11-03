import pymysql
import pandas as pd

# Setup MySQL connection
db = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Create a cursor object and execute query
cursor = db.cursor()
query = """
        SELECT
            100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
            ELSE 0
            END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
        FROM
            lineitem,
            part
        WHERE
            L_PARTKEY = P_PARTKEY
            AND L_SHIPDATE >= '1995-09-01'
            AND L_SHIPDATE < '1995-10-01'
        """
try:
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["PROMO_REVENUE"])
    df.to_csv('query_output.csv', index=False)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    db.close()
