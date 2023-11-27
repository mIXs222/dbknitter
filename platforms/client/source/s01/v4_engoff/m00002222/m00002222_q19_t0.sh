# install_dependencies.sh
#!/bin/bash

# Update the package list
apt-get update

# Install pip if it's not already installed
apt-get install -y python3-pip

# Install the required python libraries
pip3 install pymysql direct_redis pandas
