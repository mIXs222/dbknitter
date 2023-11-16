# install_dependencies.sh
#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip if not installed
apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
