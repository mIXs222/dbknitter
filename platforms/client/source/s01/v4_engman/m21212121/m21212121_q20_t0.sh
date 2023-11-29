# Filename: setup.sh

#!/bin/bash

# Update to get the latest package lists
sudo apt update

# Install pip for Python3
sudo apt install python3-pip -y

# Install MongoDB driver for Python
pip3 install pymongo

# Install Redis driver for Python
# Note: The package 'direct_redis' might not exist, and this is a placeholder name for illustrative purposes.
# If there's a specific implementation your environment uses for 'direct_redis.DirectRedis', 
# you would need to include that package here instead.
pip3 install direct_redis

# Install pandas
pip3 install pandas
