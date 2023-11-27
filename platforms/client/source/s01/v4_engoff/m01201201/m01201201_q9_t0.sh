# install_dependencies.sh
#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install python packages
pip3 install pymysql pymongo pandas direct_redis
