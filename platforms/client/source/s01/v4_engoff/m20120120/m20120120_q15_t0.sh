#!/bin/bash
# install_dependencies.sh

# Update package list and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis, which might not be available via pip.
# If it is not available in the public repository, you may need to obtain it directly from its source
# Here we assume direct_redis is available on pip
pip3 install direct_redis
