# install_dependencies.sh
#!/bin/bash

# Update apt-get and Install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required packages
pip3 install pymongo redis pandas direct-redis
