# file: install_dependencies.sh

#!/bin/bash

# Update the package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas pymysql direct_redis
