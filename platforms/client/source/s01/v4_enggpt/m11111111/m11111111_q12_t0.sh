# File: install_dependencies.sh
#!/bin/bash

# Update and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
