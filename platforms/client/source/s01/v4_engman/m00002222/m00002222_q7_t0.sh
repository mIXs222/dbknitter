# install_dependencies.sh
#!/bin/bash

# Update package list and install pip if needed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required packages
pip3 install pymysql pandas redis direct_redis
