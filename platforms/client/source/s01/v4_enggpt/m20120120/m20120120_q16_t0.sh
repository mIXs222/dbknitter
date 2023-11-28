# install_dependencies.sh
#!/bin/bash

# Update repositories and install pip if it's not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct_redis
