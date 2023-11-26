# install_dependencies.sh

#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install pymysql and pandas using pip
pip3 install pymysql pandas

# Install the direct_redis package
pip3 install git+https://github.com/popaclaudiu/direct-redis.git
