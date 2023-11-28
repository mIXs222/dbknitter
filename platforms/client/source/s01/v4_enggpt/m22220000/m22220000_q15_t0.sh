# The Bash script (setup.sh)
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install python3 python3-pip -y

# Install the required Python libraries
pip3 install pymysql pandas direct_redis
