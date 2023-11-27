# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
