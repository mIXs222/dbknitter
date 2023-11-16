# install_dependencies.sh
#!/bin/bash
set -e

# Update packages and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 pip

# Install the required Python libraries
pip install pymysql pymongo

# After this script you can run the python code
# python mysql_to_mongo_query.py
