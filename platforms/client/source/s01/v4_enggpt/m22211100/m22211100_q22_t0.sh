# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install python3 python3-pip -y

# Install MySQL client and MongoDB client
apt-get install default-libmysqlclient-dev libssl-dev -y

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install the required libraries
pip3 install pymysql pymongo
