# install_dependencies.sh
#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3, pip and the necessary build tools
sudo apt-get install -y python3 python3-pip python3-dev build-essential

# Install the library dependencies using pip
pip3 install pymysql pymongo pandas direct-redis
