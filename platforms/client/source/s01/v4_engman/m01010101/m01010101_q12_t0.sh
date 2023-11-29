# install_dependencies.sh

#!/bin/bash

# Update package list and upgrade existing packages
apt-get update
apt-get upgrade -y

# Install Python3 and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install Python packages required for the script
pip3 install pymysql pymongo
