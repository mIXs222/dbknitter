# install_dependencies.sh
#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
