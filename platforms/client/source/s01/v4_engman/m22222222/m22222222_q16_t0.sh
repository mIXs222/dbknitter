# install_dependencies.sh
#!/bin/bash

# Update repository and install Python3 and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas redis direct_redis
