# install_dependencies.sh
#!/bin/bash

# Update package list
apt-get update

# Upgrade existing packages
apt-get upgrade -y

# Install Python pip
apt-get install python3-pip -y

# Install required libraries
pip3 install pymysql pymongo pandas pyarrow

# Redis installation
apt-get install redis-server -y
pip3 install direct-redis
