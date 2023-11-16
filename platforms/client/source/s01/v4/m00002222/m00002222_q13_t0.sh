#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install pandas redis direct-redis

# The bash script ends here. After running this script, run `python execute_query.py`
# to execute the query and write the output to query_output.csv
