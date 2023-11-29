# install_dependencies.sh
#!/bin/bash

# Update repositories and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis direct-redis
