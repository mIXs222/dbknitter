# install_dependencies.sh
#!/bin/bash

# Update package list and Upgrade system
apt-get update -y
apt-get upgrade -y

# Install Python3 and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
