# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
