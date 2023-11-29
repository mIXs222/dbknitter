# file: install_dependencies.sh

#!/bin/bash

# Update package list, just in case
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install direct_redis
pip3 install pandas
