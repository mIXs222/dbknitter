# setup.sh
#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install python libraries
pip3 install pymysql pymongo pandas 'direct_redis>=1.0' 'redis>=4.0'
