# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install Python pip
apt-get install -y python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
