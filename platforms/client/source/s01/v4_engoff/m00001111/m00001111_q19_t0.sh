#!/bin/bash
# install_dependencies.sh

# Update the package manager
apt-get update -y

# Install pip and Python development files (might already be installed)
apt-get install python3-pip python3-dev -y

# Install the required python packages
pip3 install pymysql pymongo pandas
