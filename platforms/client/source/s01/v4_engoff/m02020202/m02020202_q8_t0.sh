# setup.sh
#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python pip
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas pymysql redis direct-redis
