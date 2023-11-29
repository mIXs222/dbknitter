# install.sh content

#!/bin/bash

# Update package list and install pip
apt-get update
apt-get install -y python3-pip

# Make sure pandas, pymongo, and direct_redis packages are installed
pip3 install pandas pymongo direct_redis
