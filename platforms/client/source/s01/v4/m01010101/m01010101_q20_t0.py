import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(hostname='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Fetching parts with P_NAME like 'forest%'
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
part_keys = [row[0] for row in mysql_cursor.fetchall()]

# Fetching partsupp for parts which is needed
format_strings = ','.join(['%s'] * len(part_keys))
mysql_cursor.execute(f"SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY IN ({format_strings})", tuple(part_keys))
partsupp_records = mysql_cursor.fetchall()

supplier_keys = []
for record in partsupp_records:
    ps_partkey, ps_suppkey = record

    # Getting the sum of L_QUANTITY from lineitem collection in MongoDB
    l_quantity_sum = mongo_db.lineitem.aggregate([
        {
            '$match': {
                'L_PARTKEY': ps_partkey,
                'L_SUPPKEY': ps_suppkey,
                'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}
            }
        },
        {
            '$group': {
                '_id': None,
                'total_quantity': {'$sum': '$L_QUANTITY'}
            }
        }
    ])

    total_quantity = list(l_quantity_sum)[0]['total_quantity'] if l_quantity_sum else 0

    # Comparing PS_AVAILQTY to 0.5 * the sum of L_QUANTITY
    if mysql_cursor.execute("SELECT 1 FROM partsupp WHERE PS_PARTKEY = %s AND PS_SUPPKEY = %s AND PS_AVAILQTY > %s", (ps_partkey, ps_suppkey, 0.5 * total_quantity)):
        supplier_keys.append(ps_suppkey)

# Fetching suppliers who are supplying the parts
format_strings = ','.join(['%s'] * len(supplier_keys))
mysql_cursor.execute(f"SELECT S_SUPPKEY, S_NAME, S_ADDRESS FROM supplier WHERE S_SUPPKEY IN ({format_strings})", tuple(supplier_keys))
suppliers = mysql_cursor.fetchall()

# Fetching nation names for supplier nation keys
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'CANADA'")
nation_canada = mysql_cursor.fetchall()[0] if mysql_cursor.rowcount else (None, None)

# Writing to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'S_ADDRESS'])
    for supplier in suppliers:
        s_suppkey, s_name, s_address = supplier
        if s_suppkey in supplier_keys and nation_canada[0] is not None:
            writer.writerow([s_name, s_address])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
