# bash script (setup.sh)

#!/bin/bash

# Update package lists 
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python MongoDB client
pip3 install pymongo
