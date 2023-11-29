# install.sh file

#!/bin/bash

# Update package list
sudo apt update

# Install Python3 and PIP if they are not installed
sudo apt install -y python3 python3-pip

# Install PyMySQL and PyMongo Python packages
pip3 install pymysql pymongo
