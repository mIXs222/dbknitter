#!/bin/bash
set -e

# Update package lists
apt-get update -y

# Install Python and Pip if they are not available
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pandas pymysql pymongo redis direct-redis

# Note that direct-redis may need to be installed according to its availability or provided by the user.
