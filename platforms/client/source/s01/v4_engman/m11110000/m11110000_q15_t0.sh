# install_dependencies.sh

#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip if it is not installed
sudo apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python MongoDB client
pip3 install pymongo
