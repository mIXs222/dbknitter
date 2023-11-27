# install_dependencies.sh

#!/bin/bash

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MySQL client
apt-get install -y default-libmysqlclient-dev

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct-redis
