# 2. Bash Script

#!/bin/bash

# Install Python and pip if they aren't already installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pymongo pandas direct_redis
