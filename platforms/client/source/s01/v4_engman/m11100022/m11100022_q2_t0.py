# query.py
import pymysql
import pymongo
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# MongoDB query to get nation and region information
region_data = mongo_db.region.find({'R_NAME': 'EUROPE'})
europe_region_keys = [region['R_REGIONKEY'] for region in region_data]

nation_data = mongo_db.nation.find({'N_REGIONKEY': {'$in': europe_region_keys}})
nation_key_name_map = {nation['N_NATIONKEY']: nation['N_NAME'] for nation in nation_data}

# MongoDB query to get part information
part_data = mongo_db.part.find({'P_TYPE': 'BRASS', 'P_SIZE': 15},
                               {'P_PARTKEY': 1, 'P_MFGR': 1})
part_info_map = {part['P_PARTKEY']: part for part in part_data}

# SQL query to get supplier and partsupp information
mysql_cursor.execute(
    """
    SELECT ps.PS_PARTKEY, ps.PS_SUPPKEY, MIN(ps.PS_SUPPLYCOST) as min_cost
    FROM partsupp ps
    INNER JOIN supplier s ON s.S_SUPPKEY = ps.PS_SUPPKEY
    WHERE ps.PS_PARTKEY IN (%s)
    GROUP BY ps.PS_PARTKEY, ps.PS_SUPPKEY
    HAVING min_cost = (
        SELECT MIN(ps_inner.PS_SUPPLYCOST)
        FROM partsupp ps_inner
        WHERE ps_inner.PS_PARTKEY = ps.PS_PARTKEY
    )
    """ % ','.join(map(str, part_info_map.keys()))
)
parts_suppliers = mysql_cursor.fetchall()

result = []
for ps_partkey, ps_suppkey, min_cost in parts_suppliers:
    # SQL query to get more detailed supplier information
    mysql_cursor.execute(
        """
        SELECT s.S_ACCTBAL, s.S_ADDRESS, s.S_COMMENT, s.S_NAME, s.S_PHONE, s.S_NATIONKEY
        FROM supplier s
        WHERE s.S_SUPPKEY = %s
        ORDER BY s.S_ACCTBAL DESC, s.S_NAME, s.S_SUPPKEY
        """, (ps_suppkey,)
    )
    supplier_info = mysql_cursor.fetchall()
    for s_acctbal, s_address, s_comment, s_name, s_phone, s_nationkey in supplier_info:
        n_name = nation_key_name_map.get(s_nationkey, '')
        p_mfgr = part_info_map[ps_partkey]['P_MFGR']
        p_partkey = ps_partkey

        result.append([n_name, p_mfgr, p_partkey, s_acctbal, s_address, s_comment, s_name, s_phone])

# Sort results according to specified order
result.sort(key=lambda x: (x[0], -x[3], x[2], x[6]))

# Write to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    for row in result:
        csvwriter.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
