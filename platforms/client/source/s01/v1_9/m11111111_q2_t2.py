from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# splice the task into smaller units to make PyMongo queries more feasible
# since we are dealing with Multiple tables - part, supplier, partsupp, nation, and region
parts = db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}})
output_list = []
for part in parts:
    p_partkey = part['P_PARTKEY']
    partsupps = db.partsupp.find({'PS_PARTKEY': p_partkey, 'PS_SUPPLYCOST': {'$eq': db.partsupp.find({}).sort([('PS_SUPPLYCOST', 1)]).limit(1)[0]['PS_SUPPLYCOST']}})
    for partsupp in partsupps:
        s_suppkey = partsupp['PS_SUPPKEY']
        suppliers = db.supplier.find({'S_SUPPKEY': s_suppkey})
        for supplier in suppliers:
            s_nationkey = supplier['S_NATIONKEY']
            nations = db.nation.find({'N_NATIONKEY': s_nationkey})
            for nation in nations:
                n_regionkey = nation['N_REGIONKEY']
                regions = db.region.find({'R_REGIONKEY': n_regionkey, 'R_NAME': 'EUROPE'})
                for region in regions:
                    output_list.append({
                        'S_ACCTBAL': supplier['S_ACCTBAL'],
                        'S_NAME': supplier['S_NAME'],
                        'N_NAME': nation['N_NAME'],
                        'P_PARTKEY': part['P_PARTKEY'],
                        'P_MFGR': part['P_MFGR'],
                        'S_ADDRESS': supplier['S_ADDRESS'],
                        'S_PHONE': supplier['S_PHONE'],
                        'S_COMMENT': supplier['S_COMMENT']
                    })

df = pd.DataFrame(output_list)

# order by S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# save data to csv
df.to_csv("query_output.csv", sep=',',index=False)
