#!/bin/bash
# install_dependencies.sh

# Update system packages and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install MySQL client libraries dependencies
sudo apt-get install -y default-libmysqlclient-dev

# Install required Python libraries
pip3 install pymysql pandas direct-redis
