# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pandas library
pip3 install pandas

# Install direct_redis library
pip3 install git+https://github.com/hmartinezf/direct_redis.git
