#!/bin/bash

# install_dependencies.sh
# Ensure pip is installed
sudo apt-get install python3-pip -y

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install required Python packages
pip3 install pymysql direct_redis pandas
