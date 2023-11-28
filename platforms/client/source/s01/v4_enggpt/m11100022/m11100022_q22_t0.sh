# install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3 and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql

# Install direct_redis
pip3 install direct-redis

# Install pandas
pip3 install pandas
