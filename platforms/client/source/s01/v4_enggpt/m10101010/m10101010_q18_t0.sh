# install_dependencies.sh

#!/bin/bash

# Update repositories and upgrade the system
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql and pymongo
pip3 install pymysql pymongo
