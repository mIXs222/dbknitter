# install.sh

#!/bin/bash

# Update package list and install pip if not already present
apt-get update
apt-get install -y python3-pip

# Install pymongo for MongoDB connection
pip3 install pymongo
