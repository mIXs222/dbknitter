import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query MySQL for part suppliers in GERMANY and calculate total value
mysql_cur = mysql_conn.cursor()
mysql_cur.execute("""
SELECT p.PS_PARTKEY, SUM(p.PS_SUPPLYCOST * p.PS_AVAILQTY) AS total_value
FROM partsupp p, (SELECT S_SUPPKEY
                  FROM supplier
                  WHERE S_NATIONKEY = (SELECT N_NATIONKEY
                                       FROM nation
                                       WHERE N_NAME = 'GERMANY')) AS s
WHERE p.PS_SUPPKEY = s.S_SUPPKEY
GROUP BY p.PS_PARTKEY
HAVING total_value > (SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001 FROM partsupp)
ORDER BY total_value DESC;
""")

part_values = mysql_cur.fetchall()

# Close MySQL cursor and connection
mysql_cur.close()
mysql_conn.close()

# Write query's output to file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    # Write headers
    csvwriter.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    
    # Write data
    for row in part_values:
        csvwriter.writerow(row)
