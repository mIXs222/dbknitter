# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip, if not already installed
apt-get install -y python3 python3-pip

# Install MySQL client (to enable pymysql to work)
apt-get install -y default-libmysqlclient-dev build-essential

# Install pandas, pymysql, and direct_redis packages using pip
pip3 install pandas pymysql direct_redis
