# install_dependencies.sh
#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MySQL client
apt-get install -y default-mysql-client

# Install the required Python packages
pip3 install pymysql pymongo
