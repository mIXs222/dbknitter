import pymongo
import csv
from bson.code import Code

# connect to mongodb
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# get table as collection
part = db.part
supplier = db.supplier
partsupp = db.partsupp

# run mongo query
mapper = Code("""
    function() {
        if(this.P_PARTKEY == this.PS_PARTKEY && this.P_BRAND != 'Brand#45' && this.P_TYPE.substring(0,14) != 'MEDIUM POLISHED' && 
           this.P_SIZE in [49, 14, 23, 45, 19, 3, 36, 9] && this.PS_SUPPKEY not in [this.S_SUPPKEY where this.S_COMMENT.includes('Customer Complaints')]
           ){
           emit({ P_BRAND:this.P_BRAND, P_TYPE:this.P_TYPE, P_SIZE:this.P_SIZE}, {count:1});
        }
    }
    """)

reducer = Code("""
    function(key, values){
        var result = { count: 0 };
        values.forEach(function(value){
            result.count += value.count;
        });
        return result;
    }
    """)

result = partsupp.map_reduce(mapper, reducer, "myresults")
results = list(result.find())

# save to csv file
headers = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']

with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, fieldnames = headers)
    dict_writer.writeheader()
    for data in results:
        data['_id'][SUPPLIER_CNT] = data['value'][count]
        dict_writer.writerow(data['_id'])
