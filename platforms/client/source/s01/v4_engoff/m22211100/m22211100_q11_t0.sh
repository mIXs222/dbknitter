# install_dependencies.sh

#!/bin/bash

# Update package lists
apt-get update

# Install pip if it's not installed
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo direct_redis pandas
