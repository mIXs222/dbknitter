#!/bin/bash

# author:  Chirag C. Shetty (cshetty2@illinois.edu)
# date: Oct 20, 2023 

# run as:  . ./filter.sh 

# assumes: chatgpt ouput are stored in the folder ./query_output ($OUTPUT_FOLDER)
#          and folders python_code, bash_installs exist in $OUTPUT_FOLDER

# Does: extracts the python code and bash install instructions from chatgpt output
# and saves it in the above folders

OUTPUT_FOLDER="./dbknitter/query_output" 

for file in $OUTPUT_FOLDER/*; 
do   
    if [ -f "$file" ]; 
    then 
        filtered_file=$(echo "$file" | rev | cut -d'/' -f 1 | rev | cut -d'.' -f 1 ;)
        sed -n '/```python/,$p' "$file" | tail -n +2 | sed '/```/q' | head -n -1 \
                         > $OUTPUT_FOLDER/python_code/${filtered_file}.py ;    
        sed -n '/```bash/,$p' "$file" | tail -n +2 | sed '/```/q' | head -n -1 \
                         > $OUTPUT_FOLDER/bash_installs/${filtered_file}.sh ; 
    fi ; 
done
