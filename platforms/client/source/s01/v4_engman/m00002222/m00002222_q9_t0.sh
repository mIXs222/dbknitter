# bash_script.sh
#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3, pip and python3-dev (development files needed for MySQL)
sudo apt-get install python3 python3-pip python3-dev -y

# Install the required Python libraries
pip3 install pymysql pandas sqlalchemy direct-redis
