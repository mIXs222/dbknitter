# File: install_dependencies.sh
#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip3 if they are not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas
pip3 install git+https://github.com/20c/direct_redis.git
