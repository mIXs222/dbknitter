# install_dependencies.sh
#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip if it's not available
sudo apt-get install -y python3-pip

# Install MongoDB driver for Python
pip3 install pymongo

# We assume that you'll install 'direct_redis' manually as it's not a standard Python package
