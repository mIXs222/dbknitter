# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install pip if not installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
