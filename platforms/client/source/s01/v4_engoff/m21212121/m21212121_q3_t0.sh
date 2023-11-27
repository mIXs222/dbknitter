# install_dependencies.sh

#!/bin/bash

# Update package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Install Python MongoDB driver library
pip3 install pymongo

# Install redis package for connecting to Redis
pip3 install git+https://github.com/RedisLabsModules/redis-py.git

# Install Pandas library
pip3 install pandas
