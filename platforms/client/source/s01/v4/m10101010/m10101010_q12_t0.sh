# bash script stored as install_dependencies.sh
#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and pip if not installed
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
