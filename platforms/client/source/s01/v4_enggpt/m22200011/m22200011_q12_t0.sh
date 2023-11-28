# install_dependencies.sh

#!/bin/bash

# Update the package list
apt-get update

# Install MongoDB
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list
apt-get update
apt-get install -y mongodb-org

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install Python MongoDB driver (pymongo)
pip3 install pymongo
