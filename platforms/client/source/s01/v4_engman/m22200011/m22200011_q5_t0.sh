# install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install MySQL client and libraries (needed for pymysql)
sudo apt-get install -y default-libmysqlclient-dev default-mysql-client

# Install Redis
sudo apt-get install -y redis

# Upgrade pip to the latest version
python3 -m pip install --upgrade pip

# Install required Python libraries
pip3 install pymysql pandas pymongo direct_redis
