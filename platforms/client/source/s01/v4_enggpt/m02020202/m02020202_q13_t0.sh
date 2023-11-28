# install_dependencies.sh

#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pandas direct-redis
