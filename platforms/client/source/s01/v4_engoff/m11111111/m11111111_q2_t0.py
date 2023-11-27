from pymongo import MongoClient
import csv

# First, define the function that will connect to MongoDB and execute the query.
def find_minimum_cost_supplier():
    client = MongoClient('mongodb', 27017)
    db = client.tpch

    # Aggregation pipeline
    pipeline = [
        # Joining tables: partsupp -> supplier -> nation -> region
        {
            '$lookup': {
                'from': 'supplier',
                'localField': 'PS_SUPPKEY',
                'foreignField': 'S_SUPPKEY',
                'as': 'supplier'
            }
        },
        {'$unwind': '$supplier'},
        {
            '$lookup': {
                'from': 'nation',
                'localField': 'supplier.S_NATIONKEY',
                'foreignField': 'N_NATIONKEY',
                'as': 'nation'
            }
        },
        {'$unwind': '$nation'},
        {
            '$lookup': {
                'from': 'region',
                'localField': 'nation.N_REGIONKEY',
                'foreignField': 'R_REGIONKEY',
                'as': 'region'
            }
        },
        {'$unwind': '$region'},
        # Filtering for Brass parts of size 15 in the Europe region
        {
            '$match': {
                'region.R_NAME': 'EUROPE',
                'PS_PARTKEY': {
                    '$in': db.part.find({
                        'P_TYPE': 'BRASS',
                        'P_SIZE': 15
                    }, {'P_PARTKEY': 1}).distinct('P_PARTKEY')
                }
            }
        },
        # Grouping to get the minimum supply cost for each part
        {
            '$group': {
                '_id': '$PS_PARTKEY',
                'min_cost': {'$min': '$PS_SUPPLYCOST'},
                'suppliers': {'$push': '$$ROOT'}
            }
        },
        # Filter suppliers within the group that have the minimum cost
        {
            '$project': {
                'part_key': '$_id',
                'min_cost': 1,
                'suppliers': {
                    '$filter': {
                        'input': '$suppliers',
                        'as': 'supplier',
                        'cond': {'$eq': ['$$supplier.PS_SUPPLYCOST', '$min_cost']}
                    }
                }
            }
        },
        {'$unwind': '$suppliers'},
        {'$sort': {
            'suppliers.supplier.S_ACCTBAL': -1,
            'suppliers.nation.N_NAME': 1,
            'suppliers.supplier.S_NAME': 1,
            'part_key': 1
        }},
        # Final projection for required fields
        {
            '$project': {
                'S_ACCTBAL': '$suppliers.supplier.S_ACCTBAL',
                'S_NAME': '$suppliers.supplier.S_NAME',
                'N_NAME': '$suppliers.nation.N_NAME',
                'P_PARTKEY': '$part_key',
                'P_MFGR': {
                    '$arrayElemAt': [
                        db.part.find({'P_PARTKEY': '$part_key'}, {'P_MFGR': 1}), 0
                    ]
                },
                'S_ADDRESS': '$suppliers.supplier.S_ADDRESS',
                'S_PHONE': '$suppliers.supplier.S_PHONE',
                'S_COMMENT': '$suppliers.supplier.S_COMMENT',
                '_id': 0
            }
        }
    ]

    results = list(db.partsupp.aggregate(pipeline))

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR',
                      'S_ADDRESS', 'S_PHONE', 'S_COMMENT']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in results:
            # Fetch P_MFGR since it was deferred
            p_mfgr_result = db.part.find_one({'P_PARTKEY': data['P_PARTKEY']}, {'P_MFGR': 1})
            data['P_MFGR'] = p_mfgr_result['P_MFGR'] if p_mfgr_result else None

            writer.writerow(data)

    print("Query results are written to query_output.csv")

# Call the function to execute the query when this script is run.
if __name__ == '__main__':
    find_minimum_cost_supplier()
