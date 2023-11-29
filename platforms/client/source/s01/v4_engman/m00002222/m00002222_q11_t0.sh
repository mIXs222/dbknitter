# install_dependencies.sh
#!/bin/bash

# Update package lists
apt-get update

# Install Python pip
apt-get install -y python3-pip

# Install required python packages
pip3 install pymysql direct_redis pandas
