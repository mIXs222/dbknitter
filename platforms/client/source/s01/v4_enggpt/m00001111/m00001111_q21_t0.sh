# Bash script (`install_deps.sh`) to install dependencies

#!/bin/bash

# Update the package list
apt-get update

# Install Python3 and Pip if not already present
apt-get install -y python3 python3-pip

# Install pymysql and pymongo Python packages
pip3 install pymysql pymongo
