#!/bin/bash
###
 # @Author: fhx hanxif01@gmail.com
 # @Date: 2023-11-02 21:13:29
 # @LastEditors: fhx hanxif01@gmail.com
 # @LastEditTime: 2023-11-12 15:59:44
 # @FilePath: /dbknitter/dbknitter/filter2.sh
 # @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
### 

# author:  Chirag C. Shetty (cshetty2@illinois.edu)
# date: Oct 20, 2023 

# run as:  . ./filter.sh 

# assumes: chatgpt ouput are stored in the folder ./query_output ($OUTPUT_FOLDER)
#          and folders python_code, bash_installs exist in $OUTPUT_FOLDER

# Does: extracts the python code and bash install instructions from chatgpt output
# and saves it in the above folders

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 1 ]
then
  echo "Require 1 argument (DATA_ROOT), $# provided"
  echo "Example: tpch_init.sh /path/to/source"
  echo "Example: tpch_init.sh /path/to/data nation,region,part,supplier,partsupp,customer,orders,lineitem"
  exit 1
fi

DATA_ROOT=$1
echo "Using DATA_ROOT=${DATA_ROOT}"

HEAD=head
# HEAD=ghead  # MacOS

for source in $DATA_ROOT/*.txt; do   
    fname=$(basename $source)
    fbname=${fname%.*}
    if [ -f "$source" ]; 
    then 
        echo "${source} -> ${DATA_ROOT}/${fbname}.[py|sh]"
        sed -n '/```python/,$p' "$source" | tail -n +2 | sed '/```/q' | $HEAD -n -1 \
                         > $DATA_ROOT/${fbname}.py ;    
        sed -n '/```bash/,$p' "$source" | tail -n +2 | sed '/```/q' | $HEAD -n -1 \
                         > $DATA_ROOT/${fbname}.sh ; 
    fi ; 
done
