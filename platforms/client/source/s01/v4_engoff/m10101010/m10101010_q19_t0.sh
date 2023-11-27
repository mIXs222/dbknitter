# Bash script in 'install_dependencies.sh'

#!/bin/bash
# Update package lists
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Ensure pip is up to date
pip3 install --upgrade pip

# Install pymysql and pymongo
pip3 install pymysql
pip3 install pymongo
