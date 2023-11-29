#!/bin/bash
# install_dependencies.sh

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas

# Install the direct_redis library - assuming it has to be collected from a specific source
# as it is not available through the standard pip repository.
# This line should be modified according to the actual source of the direct_redis library.
pip3 install git+https://github.com/username/direct_redis.git
