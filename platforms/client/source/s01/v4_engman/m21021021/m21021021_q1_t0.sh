# install.sh
#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
