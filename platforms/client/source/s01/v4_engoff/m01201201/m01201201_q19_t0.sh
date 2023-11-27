# install_dependencies.sh
#!/bin/bash
set -e

# Update system and install python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymongo pandas direct_redis
