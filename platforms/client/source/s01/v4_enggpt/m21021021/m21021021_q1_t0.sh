# install.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
