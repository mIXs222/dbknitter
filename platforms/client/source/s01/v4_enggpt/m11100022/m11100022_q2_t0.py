import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query MySQL for suppliers and partsupp
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(
            "SELECT s.S_SUPPKEY, s.S_ACCTBAL, s.S_NAME, s.S_ADDRESS, "
            "s.S_PHONE, s.S_COMMENT, p.PS_PARTKEY "
            "FROM supplier AS s JOIN partsupp AS p ON s.S_SUPPKEY = p.PS_SUPPKEY "
            "WHERE p.PS_PARTKEY IN "
            "(SELECT P_PARTKEY FROM part WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS%') "
            "AND s.S_NATIONKEY IN "
            "(SELECT N_NATIONKEY FROM nation WHERE N_REGIONKEY = "
            "(SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'))"
        )
        mysql_data = cursor.fetchall()

# Query MongoDB for part, nation
parts_query = {'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}}
part_data = list(mongodb_db.part.find(parts_query, {'_id': 0}))

nation_query = {'N_REGIONKEY': {'$in': mongodb_db.region.find({'R_NAME': 'EUROPE'}, {'_id': 0, 'R_REGIONKEY': 1})}}
nation_data = list(mongodb_db.nation.find(nation_query, {'_id': 0}))

# Create a mapping of part keys and nation keys for lookup
part_mapping = {p['P_PARTKEY']: p for p in part_data}
nation_mapping = {n['N_NATIONKEY']: n['N_NAME'] for n in nation_data}

# Combine data from MySQL and MongoDB for the final result
results = []
for row in mysql_data:
    part_key = row[6]
    nation_key = row[5]
    if part_key in part_mapping:
        part = part_mapping[part_key]
        supplier_data = {
            'S_SUPPKEY': row[0],
            'S_ACCTBAL': row[1],
            'S_NAME': row[2],
            'S_ADDRESS': row[3],
            'S_PHONE': row[4],
            'S_COMMENT': row[5],
            'P_PARTKEY': part_key,
            'P_MFGR': part['P_MFGR'],
            'P_SIZE': part['P_SIZE'],
            'N_NAME': nation_mapping.get(nation_key, 'UNKNOWN')
        }
        results.append(supplier_data)

# Sorting the results
results.sort(key=lambda x: (-x['S_ACCTBAL'], x['N_NAME'], x['S_NAME'], x['P_PARTKEY']))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE',
                  'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for result in results:
        writer.writerow(result)
