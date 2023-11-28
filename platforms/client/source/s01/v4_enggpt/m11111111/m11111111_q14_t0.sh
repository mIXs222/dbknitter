# install_dependencies.sh
#!/bin/bash

# Update the package list and upgrade existing packages
sudo apt-get update && sudo apt-get upgrade -y

# Install Python and pip if they aren't already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo to interact with MongoDB from Python
pip3 install pymongo
