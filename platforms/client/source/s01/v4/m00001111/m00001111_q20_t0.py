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

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch part keys with P_NAME like 'forest%'
cursor = mysql_conn.cursor()
cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
part_keys = [row[0] for row in cursor.fetchall()]

# Fetch supplier details and nation keys from MySQL
cursor.execute("""
    SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_NATIONKEY
    FROM supplier AS s
    JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
    WHERE n.N_NAME = 'CANADA'
""")
suppliers = cursor.fetchall()
cursor.close()
mysql_conn.close()

# Mapping from supplier keys to supplier info for eligible suppliers
eligible_suppliers = {}
for supp_key, name, address, nation_key in suppliers:
    eligible_suppliers[supp_key] = {
        'S_NAME': name,
        'S_ADDRESS': address,
        'S_NATIONKEY': nation_key
    }

# Pull from MongoDB and filter partsupp
partsupp_collection = mongo_db['partsupp']
partsupp_docs = partsupp_collection.find({
    'PS_PARTKEY': {'$in': part_keys},
    'PS_SUPPKEY': {'$in': list(eligible_suppliers.keys())}
})

# Filter lineitem collection from MongoDB by partsupp and date range
lineitem_collection = mongo_db['lineitem']
half_qty_by_suppart = {}
for partsupp in partsupp_docs:
    ps_suppkey = partsupp['PS_SUPPKEY']
    ps_partkey = partsupp['PS_PARTKEY']
    sum_qty = lineitem_collection.aggregate([
        {'$match': {
            'L_PARTKEY': ps_partkey,
            'L_SUPPKEY': ps_suppkey,
            'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}
        }},
        {'$group': {
            '_id': None,
            'total_qty': {'$sum': '$L_QUANTITY'}
        }}
    ])
    sum_qty = next(sum_qty, {}).get('total_qty', 0)
    half_qty_by_suppart[(ps_partkey, ps_suppkey)] = 0.5 * sum_qty

# Filter suppliers by PS_AVAILQTY and construct result list
result = []
for partsupp in partsupp_docs:
    ps_suppkey = partsupp['PS_SUPPKEY']
    ps_partkey = partsupp['PS_PARTKEY']
    ps_availqty = partsupp['PS_AVAILQTY']
    if ps_availqty > half_qty_by_suppart.get((ps_partkey, ps_suppkey), 0):
        supplier_info = eligible_suppliers.get(ps_suppkey)
        if supplier_info:
            result.append((supplier_info['S_NAME'], supplier_info['S_ADDRESS']))

# Sort results by supplier name
result_sorted = sorted(result, key=lambda r: r[0])

# Write query output to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_NAME', 'S_ADDRESS'])
    for row in result_sorted:
        writer.writerow(row)
