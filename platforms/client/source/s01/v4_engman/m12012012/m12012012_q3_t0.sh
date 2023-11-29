# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install MySQL Client
apt-get install -y default-mysql-client

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis
