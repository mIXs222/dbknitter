# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install pip and Python MongoDB driver
apt-get install -y python3-pip
pip3 install pymongo

# Run the Python script to execute the query
python3 revenue_query.py
