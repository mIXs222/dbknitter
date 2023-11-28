# file: setup.sh

#!/bin/bash

# Update package index
sudo apt-get update

# Install Python3 if it's not available
sudo apt-get install -y python3

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pandas pymongo redis direct_redis
